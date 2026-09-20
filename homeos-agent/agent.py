#!/usr/bin/env python3
"""
HOME OS — Agente Edge v0.1

Roda ao lado do Home Assistant. Responsabilidades:

  1. Escutar o event bus do HA (websocket)
  2. Filtrar para a lista de entidades de interesse
  3. CLASSIFICAR A AUTORIA de cada evento (humano vs sistema)
  4. Detectar overrides — quando um humano desfaz o sistema
  5. Bufferizar em SQLite e enviar em lote para a nuvem
  6. Heartbeat

Princípios de projeto:
  - Somente conexões de SAÍDA. Nenhuma porta aberta na casa do cliente.
  - Funciona offline. O buffer aguenta uma semana sem internet.
  - Vídeo e áudio nunca passam por aqui.
  - Se a nuvem morrer, a casa continua funcionando (o HA é autônomo).
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path

import httpx
import websockets
import yaml

log = logging.getLogger("homeos")

AGENT_VERSION = "0.2.0"

# =====================================================================
# A REGRA CENTRAL DO PRODUTO
# =====================================================================
#
# O Home Assistant carimba cada mudança de estado com um "context":
#
#   context.user_id  preenchido  →  UM HUMANO fez isso
#   context.user_id  nulo        →  automação, agenda ou integração
#
# Esse único campo é o que separa "o morador acendeu a luz" de "o sistema
# acendeu a luz". Sem ele não existe aprendizado de preferência — apenas
# um log de estados sem sentido.
# =====================================================================

WINDOW_OVERRIDE_S = 1800
DEBOUNCE_S = 30
BATCH_SIZE = 500
FLUSH_INTERVAL_S = 300
BUFFER_RETENTION_DAYS = 7
# O inventario muda quando alguem empareha ou renomeia um aparelho -- raro.
# Seis horas chega. Subscrever `device_registry_updated` daria sincronia
# imediata a troco de gerir re-fetch por evento; nao se paga a esta cadencia.
INVENTORY_INTERVAL_S = 6 * 3600
# A saude da frota le o ESTADO das entidades, nao o registo -- sao duas
# chamadas diferentes ao HA. A cadencia acompanha a do `fleet_health`, que
# grava uma linha a cada 15 minutos: calcular mais vezes seria deitar fora
# o resultado.
HEALTH_INTERVAL_S = 15 * 60
LOW_BATTERY_PCT = 20
# Quanto tempo uma entidade fica `unavailable` antes de virar incidente.
# O ZHA marca `unavailable` sozinho ao fim de um checkin falhado, e um
# aparelho a reiniciar volta em segundos -- avisar a cada soluco treinava
# o operador a ignorar o aviso. 30 min e curto o suficiente para agir no
# mesmo dia e longo o suficiente para nao ser ruido.
OFFLINE_ALERTA_S = 30 * 60
# Quando uma passagem FALHA, o intervalo normal nao se aplica.
# Apanhado na primeira instalacao a serio: o agente arrancou antes de a
# rota para o HA estar de pe, a sincronizacao de inventario falhou, e o
# loop foi dormir SEIS HORAS -- uma casa acabada de instalar ficava sem
# inventario nenhum durante um turno inteiro, e a T6 acusaria todas as
# suas entidades como fantasmas.
RETRY_INTERVAL_S = 60

# Os unicos atributos que interessam ao produto. O Home Assistant emite um
# `state_changed` sempre que QUALQUER atributo muda -- e a maioria das
# entidades tem dezenas que nunca olhamos.
ATTRS_RELEVANTES = (
    "brightness", "temperature", "color_temp", "position",
    "current_temperature", "hvac_action", "unit_of_measurement",
)


def attrs_relevantes(estado: dict | None) -> dict:
    return {k: v for k, v in ((estado or {}).get("attributes") or {}).items()
            if k in ATTRS_RELEVANTES}


@dataclass
class Config:
    home_id: str
    api_key: str
    cloud_url: str          # base das Edge Functions: https://<ref>.supabase.co/functions/v1
    ha_url: str = "ws://supervisor/core/websocket"
    ha_token: str = ""
    db_path: str = "/data/homeos.db"
    entity_allowlist: list = field(default_factory=list)
    # `camera.` e `media_player.` estao aqui por PRIVACIDADE. Os restantes
    # por serem ruido: medido contra a casa de Coppet (2026-09-08), 203
    # entidades no registo das quais 38 sao `update.`/`button.`/`tts.` e
    # afins -- um `button.<sensor>_identifier` do ZHA nunca muda de estado
    # (e um botao), e um `update.` diz que ha uma versao nova de um add-on.
    # Nenhum deles descreve o que acontece na casa.
    #
    # `person.` NAO entra aqui: presenca e central para o produto.
    entity_denylist: list = field(default_factory=lambda: [
        "camera.", "media_player.",
        "update.", "button.", "tts.", "stt.", "conversation.", "todo.",
    ])

    # Caminhos por ordem de preferencia. O Supervisor do Home Assistant
    # escreve as opcoes do add-on em `/data/options.json` -- JSON, e nao
    # o `options.yaml` que estava aqui codificado. O agente nunca teria
    # encontrado a sua propria configuracao a correr como add-on, que e a
    # unica forma de o instalar num HA Green (HAOS nao aceita systemd).
    # O .yaml fica para quem o corra a mao fora do Supervisor.
    CAMINHOS = ("/data/options.json", "/data/options.yaml")

    @classmethod
    def load(cls, path: str | None = None) -> "Config":
        # `HOMEOS_CONFIG` existe para correr o agente FORA do Supervisor --
        # numa maquina de operador, apontando `ha_url` ao HA pela rede e
        # usando um token de longa duracao. E o mesmo codigo; so muda de
        # onde vem a configuracao.
        caminhos = ((path,) if path
                    else tuple(c for c in (os.environ.get("HOMEOS_CONFIG"),
                                           *cls.CAMINHOS) if c))
        raw = None
        for candidato in caminhos:
            try:
                with open(candidato) as f:
                    # yaml.safe_load le JSON tambem -- JSON e um subconjunto
                    # de YAML. Uma so funcao para os dois formatos.
                    raw = yaml.safe_load(f)
                break
            except FileNotFoundError:
                continue
        if raw is None:
            raise FileNotFoundError(
                f"configuracao nao encontrada em {', '.join(str(c) for c in caminhos)}")

        # Dentro de um add-on o token do HA nao esta nas opcoes: o
        # Supervisor injecta-o no ambiente. Poe-lo nas opcoes obrigaria a
        # guardar um segredo em texto no ficheiro de configuracao do
        # add-on, visivel na UI do Home Assistant.
        raw.setdefault("ha_token", os.environ.get("SUPERVISOR_TOKEN", ""))

        # Opcoes que o Supervisor acrescenta e que nao sao nossas (ex.:
        # `log_level` do schema do add-on) nao podem rebentar o arranque.
        conhecidas = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in conhecidas})


SCHEMA = """
create table if not exists outbox (
    id          text primary key,
    kind        text not null,
    payload     text not null,
    created_at  real not null,
    attempts    integer not null default 0
);
create index if not exists outbox_kind_created on outbox(kind, created_at);

create table if not exists last_system_action (
    entity_id   text primary key,
    state       text,
    attrs       text,
    ts          real not null,
    automation  text
);
"""


class Buffer:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(SCHEMA)
        self.db.commit()

    def push(self, kind: str, payload: dict) -> None:
        self.db.execute(
            "insert into outbox(id, kind, payload, created_at) values (?,?,?,?)",
            (str(uuid.uuid4()), kind, json.dumps(payload), time.time()),
        )
        self.db.commit()

    def take(self, kind: str, limit: int) -> list[tuple[str, dict]]:
        rows = self.db.execute(
            "select id, payload from outbox where kind=? order by created_at limit ?",
            (kind, limit),
        ).fetchall()
        return [(r[0], json.loads(r[1])) for r in rows]

    def ack(self, ids: list[str]) -> None:
        self.db.executemany("delete from outbox where id=?", [(i,) for i in ids])
        self.db.commit()

    def bump_attempts(self, ids: list[str]) -> None:
        self.db.executemany(
            "update outbox set attempts = attempts + 1 where id=?", [(i,) for i in ids]
        )
        self.db.commit()

    def record_system_action(self, entity_id, state, attrs, automation=None) -> None:
        self.db.execute(
            """insert into last_system_action(entity_id, state, attrs, ts, automation)
               values (?,?,?,?,?)
               on conflict(entity_id) do update set
                 state=excluded.state, attrs=excluded.attrs,
                 ts=excluded.ts, automation=excluded.automation""",
            (entity_id, state, json.dumps(attrs), time.time(), automation),
        )
        self.db.commit()

    def get_system_action(self, entity_id: str):
        row = self.db.execute(
            "select state, attrs, ts, automation from last_system_action where entity_id=?",
            (entity_id,),
        ).fetchone()
        if not row:
            return None
        return {"state": row[0], "attrs": json.loads(row[1] or "{}"),
                "ts": row[2], "automation": row[3]}

    def prune(self) -> int:
        cutoff = time.time() - BUFFER_RETENTION_DAYS * 86400
        cur = self.db.execute("delete from outbox where created_at < ?", (cutoff,))
        self.db.commit()
        return cur.rowcount

    def depth(self) -> int:
        return self.db.execute("select count(*) from outbox").fetchone()[0]


def classify_source(context: dict | None, attrs: dict) -> str:
    """Quem causou esta mudança de estado?"""
    if not context:
        return "unknown"
    if context.get("user_id"):
        return "user"
    if context.get("parent_id"):
        return "automation"
    if attrs.get("_source_hint") == "schedule":
        return "schedule"
    return "system"


def numeric_delta(expected: dict, applied: dict) -> dict:
    """Diferença legível entre o que o sistema pôs e o que o humano quis."""
    out = {}
    for key in ("brightness", "temperature", "color_temp", "position"):
        e, a = expected.get(key), applied.get(key)
        if isinstance(e, (int, float)) and isinstance(a, (int, float)) and e != a:
            out[key] = {"from": e, "to": a, "delta": round(a - e, 2)}
    return out


def detect_override(buffer: Buffer, entity_id: str, new_state: str,
                    attrs: dict, source: str,
                    ha_user_id: str | None = None) -> dict | None:
    """
    Um override existe quando:
      - a mudança veio de um HUMANO, e
      - o SISTEMA mexeu nessa mesma entidade há menos de 30 min, e
      - o resultado é diferente do que o sistema tinha aplicado.

    delay_seconds é o sinal mais informativo do dataset:
      < 60s   rejeição forte
      1-10min ajuste de preferência
      > 30min ruído — descartado aqui, antes de sair de casa
    """
    if source != "user":
        return None

    prior = buffer.get_system_action(entity_id)
    if not prior:
        return None

    delay = time.time() - prior["ts"]
    if delay > WINDOW_OVERRIDE_S:
        return None

    expected = {"state": prior["state"], **prior["attrs"]}
    applied = {"state": new_state, **attrs}

    if expected.get("state") == applied.get("state"):
        delta = numeric_delta(prior["attrs"], attrs)
        if not delta:
            return None            # humano confirmou o sistema: não é override
    else:
        delta = numeric_delta(prior["attrs"], attrs)

    now = datetime.now(timezone.utc)
    return {
        "entity_id": entity_id,
        "ts": now.isoformat(),
        # QUAL humano corrigiu, nao so que foi um humano. Sem isto o corpus
        # mistura as preferencias de quem partilha a casa. Ver a migracao
        # 20260906100000_autoria_ha.sql.
        "ha_user_id": ha_user_id,
        "automation_ref": prior["automation"],
        "expected_state": expected,
        "applied_state": applied,
        "delay_seconds": int(delay),
        "context": {
            "hour": now.astimezone().hour,
            "weekday": now.astimezone().weekday(),
            "delta": delta,
        },
    }


def build_inventory(devices: list, entities: list, areas: list, floors: list,
                    keep=lambda entity_id: True) -> dict:
    """
    Converte os quatro registos do Home Assistant no payload da nuvem.

    O HA guarda `area_id` nos aparelhos e nas entidades, e os nomes vivem
    em dois registos a parte. A resolucao faz-se AQUI: replicar os
    registos de areas e andares na nuvem seria guardar duas tabelas para
    responder a uma pergunta de texto.

    O filtro `keep` e o MESMO que decide que eventos saem de casa, e isso
    e deliberado: a denylist tem `camera.` e `media_player.` por razoes de
    privacidade, e um inventario sem filtro anunciaria a existencia de
    cada camara da casa mesmo nunca enviando um unico evento dela.
    """
    floor_name = {f.get("floor_id"): f.get("name") for f in floors}
    area_info = {a.get("area_id"): (a.get("name"), floor_name.get(a.get("floor_id")))
                 for a in areas}

    out_entities = []
    devices_usados = set()
    for e in entities:
        entity_id = e.get("entity_id")
        if not entity_id or not keep(entity_id):
            continue
        area_override = area_info.get(e.get("area_id"), (None, None))[0]
        out_entities.append({
            "entity_id": entity_id,
            # `name` e o nome dado pelo utilizador; `original_name` o que a
            # integracao propos. O primeiro ganha quando existe.
            "name": e.get("name") or e.get("original_name"),
            "ha_device_id": e.get("device_id"),
            "area_override": area_override,
            "enabled": e.get("disabled_by") is None,
        })
        if e.get("device_id"):
            devices_usados.add(e["device_id"])

    out_devices = []
    for d in devices:
        # Um aparelho cujas entidades foram todas filtradas nao vai --
        # senao o filtro de privacidade das entidades nao valeria nada.
        if d.get("id") not in devices_usados:
            continue
        area, floor = area_info.get(d.get("area_id"), (None, None))
        out_devices.append({
            "ha_device_id": d.get("id"),
            "name": d.get("name_by_user") or d.get("name"),
            "area": area,
            "floor": floor,
            "manufacturer": d.get("manufacturer"),
            "model": d.get("model"),
        })

    return {"devices": out_devices, "entities": out_entities}


def _battery_key(entity_id: str) -> str:
    """
    Chave de deduplicacao de leituras de bateria.

    O mesmo aparelho aparece duas vezes: o ZHA expoe um
    `sensor.<slug>_battery` com o valor no estado, e a entidade principal
    (`binary_sensor.<slug>`) traz `battery_level` nos atributos. Contar as
    duas daria o dobro dos aparelhos a precisar de pilha.

    Deduplicar pelo slug sem o sufixo `_battery` resolve-o sem uma segunda
    chamada ao HA. Heuristica de texto, com um tecto conhecido: um
    aparelho cujo sensor de bateria tenha sido renomeado a mao para algo
    que nao acabe em `_battery` volta a contar duas vezes. A alternativa
    exacta e cruzar com o entity_registry pelo `device_id` -- vale a pena
    se a contagem alguma vez for usada para facturar, nao para dizer
    "vai trocar pilhas".
    """
    slug = entity_id.split(".", 1)[-1]
    return slug[: -len("_battery")] if slug.endswith("_battery") else slug


def summarize_health(states: list, keep=lambda entity_id: True) -> dict:
    """
    Conta o que o `fleet_health` precisa e que o registo de entidades nao
    sabe: quantas entidades estao mudas e quantos aparelhos tem a pilha a
    acabar.

    SO `unavailable` CONTA, e isso foi uma correccao.
    A primeira versao contava tambem `unknown`, com o argumento de que as
    duas significam "nao esta a dar dados". Medido contra a casa real de
    Coppet antes de instalar o agente: 23 entidades em `unknown` e ZERO em
    `unavailable` -- 13 `button.*_identifier` (um botao nao tem estado),
    4 `person.*` sem device tracker, 4 `update.*`. Ou seja, o campo
    nasceria com um alarme permanentemente aceso sobre uma casa saudavel,
    e a unica coisa que isso ensina e a ignora-lo.
    `unavailable` e o sinal real: a integracao PERDEU um aparelho que
    antes tinha.
    """
    indisponiveis = 0
    baterias_fracas = set()

    for st in states:
        entity_id = st.get("entity_id")
        if not entity_id or not keep(entity_id):
            continue

        if st.get("state") == "unavailable":
            indisponiveis += 1

        attrs = st.get("attributes") or {}
        nivel = attrs.get("battery_level")
        if nivel is None and attrs.get("device_class") == "battery":
            try:
                nivel = float(st.get("state"))
            except (TypeError, ValueError):
                nivel = None

        if isinstance(nivel, (int, float)) and nivel < LOW_BATTERY_PCT:
            baterias_fracas.add(_battery_key(entity_id))

    return {"entities_unavailable": indisponiveis,
            "low_batteries": len(baterias_fracas)}


class Agent:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.buffer = Buffer(cfg.db_path)
        self.debounce: dict[str, float] = {}
        self.ha_version = "unknown"
        self._msg_id = 0
        # Ultima leitura conhecida. Zeros ate a primeira passagem -- o
        # heartbeat bate ao minuto e nao pode esperar por ela.
        self.health = {"entities_unavailable": 0, "low_batteries": 0}
        # entity_id -> instante em que ficou indisponivel. Vive em memoria
        # de proposito: um agente reiniciado volta a contar do zero, o que
        # e o comportamento certo -- nao sabe ha quanto tempo o aparelho
        # esta mudo, e inventar um numero seria pior que recomecar.
        self.offline_desde: dict[str, float] = {}
        self.offline_avisados: set[str] = set()
        # entity_id -> nome do APARELHO a que pertence, preenchido pela
        # sincronizacao de inventario. Um Sonoff expoe 4 a 6 entidades, e
        # sem este mapa o aviso diz "4 aparelhos sem responder" quando e
        # um so -- com quatro entity_id tecnicos em vez de "Sensor de
        # presenca do quarto".
        self.aparelho_de: dict[str, str] = {}

    def interesting(self, entity_id: str) -> bool:
        if any(entity_id.startswith(p) for p in self.cfg.entity_denylist):
            return False
        if not self.cfg.entity_allowlist:
            return True
        return any(entity_id.startswith(p) or entity_id == p
                   for p in self.cfg.entity_allowlist)

    def next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def listen_ha(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.cfg.ha_url, max_size=8_000_000) as ws:
                    hello = json.loads(await ws.recv())
                    self.ha_version = hello.get("ha_version", "unknown")

                    await ws.send(json.dumps(
                        {"type": "auth", "access_token": self.cfg.ha_token}))
                    auth = json.loads(await ws.recv())
                    if auth.get("type") != "auth_ok":
                        raise RuntimeError(f"auth recusada pelo HA: {auth}")

                    await ws.send(json.dumps({
                        "id": self.next_id(),
                        "type": "subscribe_events",
                        "event_type": "state_changed",
                    }))
                    log.info("conectado ao HA %s", self.ha_version)
                    backoff = 1

                    async for raw in ws:
                        try:
                            self.handle(json.loads(raw))
                        except Exception:
                            log.exception("erro tratando evento")

            except Exception as e:
                log.warning("conexão HA caiu (%s) — retry em %ss", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def handle(self, msg: dict) -> None:
        if msg.get("type") != "event":
            return
        ev = msg["event"]
        if ev.get("event_type") != "state_changed":
            return

        data = ev["data"]
        entity_id = data.get("entity_id", "")
        if not self.interesting(entity_id):
            return

        new = data.get("new_state") or {}
        old = data.get("old_state") or {}
        attrs = attrs_relevantes(new)

        # NADA DO QUE NOS INTERESSA MUDOU.
        # A condicao anterior era `not new.get("attributes")`, que nunca e
        # verdadeira: toda a entidade do HA tem atributos (friendly_name,
        # icon, device_class...). O filtro de repeticao nunca funcionou.
        #
        # MEDIDO em Coppet apos 9,6 dias: `sun.sun` sozinho produziu 1269
        # dos 5443 eventos -- 23% de toda a base -- concentrados num minuto
        # por dia, quando o nascer do sol faz a elevacao e o azimute
        # mudarem centenas de vezes seguidas. Nenhum desses atributos esta
        # em ATTRS_RELEVANTES, e o estado (`above_horizon`) nao muda: sao
        # 1269 linhas identicas a dizer a mesma coisa.
        if (new.get("state") == old.get("state")
                and attrs == attrs_relevantes(old)):
            return
        ctx = ev.get("context") or {}
        source = classify_source(ctx, attrs)
        # O user_id nao serve so para decidir `source`: e o autor, e viaja
        # ate a nuvem. Nulo em tudo o que nao e accao humana.
        ha_user_id = ctx.get("user_id")

        # O debounce existe contra ruído de MÁQUINA — sensores a oscilar,
        # integrações a repetir estado. Nenhum desses é 'user'.
        #
        # NÃO MOVER este bloco para cima do classify_source. Parece uma
        # optimização óbvia (descartar cedo, poupar trabalho) e destrói o
        # produto: a correção humana chega segundos depois da ação do
        # sistema, e um debounce aplicado antes de se conhecer a autoria
        # engole exactamente a janela onde vive o override — abaixo de 60s,
        # a "rejeição forte", o sinal mais informativo do dataset.
        #
        # Também não usar uma janela mais curta para 'user': qualquer valor
        # seria arbitrário e descartaria dados reais sem critério. Uma ação
        # humana é sempre intencional e nunca é rajada — se alguém carregar
        # duas vezes no interruptor, as duas contam.
        #
        # Coberto por test_handle_sem_debounce_em_acao_humana.
        if source != "user":
            now = time.time()
            if now - self.debounce.get(entity_id, 0) < DEBOUNCE_S:
                return
            self.debounce[entity_id] = now

        self.buffer.push("event", {
            "entity_id": entity_id,
            "domain": entity_id.split(".")[0],
            "ts": new.get("last_changed") or datetime.now(timezone.utc).isoformat(),
            "old_state": old.get("state"),
            "new_state": new.get("state"),
            "attrs": attrs,
            "source": source,
            "ha_user_id": ha_user_id,
            "context_id": ctx.get("id"),
        })

        if source in ("automation", "schedule", "system"):
            self.buffer.record_system_action(
                entity_id, new.get("state"), attrs,
                automation=ctx.get("parent_id"))
            return

        override = detect_override(self.buffer, entity_id,
                                   new.get("state"), attrs, source,
                                   ha_user_id=ha_user_id)
        if override:
            log.info("override: %s corrigido em %ss", entity_id,
                     override["delay_seconds"])
            self.buffer.push("override", override)

    async def flush_loop(self) -> None:
        # NOTA: o home_id enviado no header X-Home-Id e no corpo do payload é
        # DECORATIVO. A Edge Function ignora ambos e deriva o home_id da linha
        # de edge_agents correspondente à api_key. Nunca tratar como autoritativo.
        headers = {
            "Authorization": f"Bearer {self.cfg.api_key}",
            "X-Home-Id": self.cfg.home_id,
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                await asyncio.sleep(FLUSH_INTERVAL_S)
                # `ingest-events`, com HIFEN. Uma Edge Function chamada
                # `ingest-events` serve-se em /functions/v1/ingest-events;
                # o `ingest/events` que estava aqui daria 404 em todas as
                # tentativas, e o buffer encheria sem ninguem perceber
                # porque 404 nao e 403 e o agente nunca desiste.
                for kind, path in (("override", "ingest-overrides"),
                                   ("event", "ingest-events")):
                    await self.flush(client, headers, kind, path)
                removed = self.buffer.prune()
                if removed:
                    log.warning("buffer podado: %s registros expirados", removed)

    async def flush(self, client, headers, kind: str, path: str) -> None:
        while True:
            batch = self.buffer.take(kind, BATCH_SIZE)
            if not batch:
                return
            ids = [b[0] for b in batch]
            payload = {"home_id": self.cfg.home_id, "items": [b[1] for b in batch]}
            try:
                r = await client.post(f"{self.cfg.cloud_url}/{path}",
                                      headers=headers, json=payload)
                if r.status_code in (200, 201, 202):
                    self.buffer.ack(ids)
                    log.debug("enviados %s %s", len(ids), kind)
                elif r.status_code in (401, 403):
                    log.error("credencial rejeitada — parando envio")
                    return
                else:
                    log.warning("nuvem respondeu %s — mantendo no buffer", r.status_code)
                    self.buffer.bump_attempts(ids)
                    return
            except Exception as e:
                log.warning("envio falhou (%s) — dados preservados no buffer", e)
                self.buffer.bump_attempts(ids)
                return
            if len(batch) < BATCH_SIZE:
                return

    async def heartbeat_loop(self) -> None:
        headers = {"Authorization": f"Bearer {self.cfg.api_key}",
                   "X-Home-Id": self.cfg.home_id}
        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                try:
                    await client.post(
                        f"{self.cfg.cloud_url}/ingest-heartbeat",
                        headers=headers,
                        json={
                            "home_id": self.cfg.home_id,
                            "agent_version": AGENT_VERSION,
                            "ha_version": self.ha_version,
                            "buffer_depth": self.buffer.depth(),
                            "ts": datetime.now(timezone.utc).isoformat(),
                            **self.health,
                        })
                except Exception as e:
                    log.debug("heartbeat falhou: %s", e)
                await asyncio.sleep(60)

    async def _ha_query(self, *commands: str) -> list:
        """
        Corre comandos numa ligacao DEDICADA e de curta duracao, e devolve
        os resultados pela mesma ordem.

        Nao reutiliza a ligacao do `listen_ha`: essa esta a consumir o
        stream de `state_changed` num `async for`, e intercalar pedidos
        com resposta obrigaria a encaminhar mensagens por id entre duas
        partes do codigo. Uma ligacao propria custa uma autenticacao por
        chamada e nao toca no caminho dos eventos.

        Partilhada pelo inventario e pela saude da frota de proposito --
        duas copias da sequencia de autenticacao divergiriam.
        """
        async with websockets.connect(self.cfg.ha_url, max_size=16_000_000) as ws:
            await ws.recv()                                   # auth_required
            await ws.send(json.dumps(
                {"type": "auth", "access_token": self.cfg.ha_token}))
            if json.loads(await ws.recv()).get("type") != "auth_ok":
                raise RuntimeError("auth recusada pelo HA")

            resultados = []
            for cmd in commands:
                msg_id = self.next_id()
                await ws.send(json.dumps({"id": msg_id, "type": cmd}))
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("id") != msg_id:
                        continue          # resposta de outro pedido
                    if not msg.get("success"):
                        raise RuntimeError(f"{cmd} recusado: {msg.get('error')}")
                    resultados.append(msg.get("result") or [])
                    break
            return resultados

    async def fetch_inventory(self) -> dict:
        devices, entities, areas, floors = await self._ha_query(
            "config/device_registry/list",
            "config/entity_registry/list",
            "config/area_registry/list",
            "config/floor_registry/list",
        )
        return build_inventory(devices, entities, areas, floors,
                               keep=self.interesting)

    async def avisar_offline(self, entity_ids: list[str]) -> None:
        """
        Cria uma notificacao no Home Assistant.

        E o unico sitio onde o agente ESCREVE no HA, e escreve a coisa
        mais inofensiva que existe: uma notificacao. Nao toca em nenhuma
        entidade, nao muda o estado da casa, nao interfere com a linha de
        base -- so poe um aviso no telemovel de quem instalou.

        Porque aqui e nao na nuvem: o agente e o unico que sabe em tempo
        real, e a notificacao do HA chega ao telefone sem precisar de
        servidor de e-mail, de push, nem de conta em lado nenhum.
        """
        # Agrupar por aparelho: quem le a notificacao pensa em "o sensor
        # do quarto", nao em seis entidades do registo do Home Assistant.
        aparelhos = sorted({self.aparelho_de.get(e, e) for e in entity_ids})
        nomes = ", ".join(aparelhos)
        plural = "aparelho" if len(aparelhos) == 1 else "aparelhos"
        corpo = (f"{nomes} — sem responder ha mais de "
                 f"{OFFLINE_ALERTA_S // 60} minutos.\n\n"
                 f"A causa mais comum e {'este ' + plural + ' ter' if len(aparelhos) == 1 else 'estes ' + plural + ' terem'} "
                 "sido desligados da tomada. Se foi de proposito, ignore este aviso.")
        url = self.cfg.ha_url.replace("ws://", "http://").replace("wss://", "https://")
        url = url.replace("/api/websocket", "").replace("/core/websocket", "")
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                await client.post(
                    f"{url}/api/services/persistent_notification/create",
                    headers={"Authorization": f"Bearer {self.cfg.ha_token}"},
                    json={"title": "Home OS: aparelho sem responder",
                          "message": corpo,
                          "notification_id": "homeos_offline"})
            log.warning("aviso de offline enviado ao HA: %s (%s entidades)",
                        nomes, len(entity_ids))
        except Exception as e:
            log.warning("nao consegui avisar o HA: %s", e)

    def registar_offline(self, states: list) -> list[str]:
        """
        Devolve as entidades que passaram o limiar AGORA -- nunca as que
        ja foram avisadas. Sem isto, o aviso repetia-se a cada 15 minutos
        durante os dois dias em que o aparelho esteve desligado.
        """
        agora = time.time()
        vivas = set()
        novos = []

        for st in states:
            entity_id = st.get("entity_id")
            if not entity_id or not self.interesting(entity_id):
                continue
            if st.get("state") == "unavailable":
                self.offline_desde.setdefault(entity_id, agora)
                if (agora - self.offline_desde[entity_id] >= OFFLINE_ALERTA_S
                        and entity_id not in self.offline_avisados):
                    self.offline_avisados.add(entity_id)
                    novos.append(entity_id)
            else:
                vivas.add(entity_id)

        # Voltou: esquecer, para que uma proxima queda volte a avisar.
        for entity_id in vivas:
            self.offline_desde.pop(entity_id, None)
            self.offline_avisados.discard(entity_id)

        return novos

    async def health_loop(self) -> None:
        """
        Mantem `self.health` actualizado para o heartbeat enviar.

        Vive num loop proprio e nao no heartbeat porque as cadencias sao
        diferentes: o heartbeat bate ao minuto (e o sinal de vida) e isto
        exige uma ligacao ao HA e a lista completa de estados. Uma leitura
        por minuto seria deitar catorze fora por cada uma usada.
        """
        while True:
            try:
                states, = await self._ha_query("get_states")
                self.health = summarize_health(states, keep=self.interesting)
                log.debug("saude: %s", self.health)
                novos = self.registar_offline(states)
                if novos:
                    await self.avisar_offline(novos)
                proximo = HEALTH_INTERVAL_S
            except Exception as e:
                # Mantem a ultima leitura conhecida: zeros novos diriam
                # "esta tudo bem" quando o que aconteceu foi nao conseguir
                # perguntar.
                log.warning("leitura de saude falhou: %s", e)
                proximo = RETRY_INTERVAL_S
            await asyncio.sleep(proximo)

    async def sync_inventory_loop(self) -> None:
        headers = {"Authorization": f"Bearer {self.cfg.api_key}",
                   "X-Home-Id": self.cfg.home_id}
        async with httpx.AsyncClient(timeout=60) as client:
            while True:
                try:
                    inv = await self.fetch_inventory()
                    # Mapa para os avisos, montado aqui porque o
                    # inventario ja foi buscado -- nao custa uma chamada
                    # extra ao HA.
                    nome_do_aparelho = {d["ha_device_id"]: d.get("name")
                                        for d in inv["devices"]}
                    self.aparelho_de = {
                        e["entity_id"]: nome_do_aparelho.get(e.get("ha_device_id"))
                                        or e.get("name") or e["entity_id"]
                        for e in inv["entities"]}
                    r = await client.post(
                        f"{self.cfg.cloud_url}/ingest-inventory",
                        headers=headers,
                        json={"home_id": self.cfg.home_id, **inv})
                    if r.status_code in (200, 201, 202):
                        log.info("inventario sincronizado: %s aparelhos, %s entidades",
                                 len(inv["devices"]), len(inv["entities"]))
                        proximo = INVENTORY_INTERVAL_S
                    else:
                        log.warning("inventario recusado: %s", r.status_code)
                        proximo = RETRY_INTERVAL_S
                except Exception as e:
                    # O inventario nao e bufferizado: nao ha nada a perder,
                    # a proxima tentativa reenvia o estado actual.
                    log.warning("sincronizacao de inventario falhou: %s", e)
                    proximo = RETRY_INTERVAL_S
                await asyncio.sleep(proximo)

    async def run(self) -> None:
        await asyncio.gather(self.listen_ha(), self.flush_loop(),
                             self.heartbeat_loop(), self.sync_inventory_loop(),
                             self.health_loop())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s")
    cfg = Config.load()
    log.info("Home OS agent — home_id=%s", cfg.home_id)
    asyncio.run(Agent(cfg).run())


if __name__ == "__main__":
    main()
