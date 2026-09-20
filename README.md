# Home OS — add-on do Home Assistant

Agente que corre ao lado do Home Assistant e envia eventos, correcoes
humanas e saude da frota para o Home OS.

## Instalar

1. No Home Assistant: **Definicoes → Add-ons → Loja de add-ons**
2. Menu dos tres pontos (canto superior direito) → **Repositorios**
3. Acrescentar: `https://github.com/coxvlf-coder/home-os-addon`
4. Fechar, recarregar a pagina, e instalar **Home OS Agent** na loja
5. Em **Configuracao**, preencher `home_id`, `api_key` e `cloud_url`
6. **Iniciar**

## O que ele faz

- Escuta o event bus do Home Assistant e classifica a AUTORIA de cada
  mudanca de estado (humano vs automacao vs sistema)
- Deteta *overrides* — quando uma pessoa corrige o que o sistema fez
- Sincroniza o inventario de aparelhos e entidades
- Envia um heartbeat com a saude da frota, e avisa no proprio Home
  Assistant quando um aparelho fica sem responder

## Principios

- **So ligacoes de saida.** Nenhuma porta aberta na casa.
- **Funciona offline.** O buffer SQLite aguenta uma semana sem internet.
- **Video e audio nunca passam por aqui.** As entidades `camera.` e
  `media_player.` estao na denylist por omissao, e nem sequer aparecem
  no inventario.
- **Se a nuvem morrer, a casa continua a funcionar.** O Home Assistant e
  autonomo; este agente so observa.

## Configuracao

| Opcao | O que e |
|---|---|
| `home_id` | UUID da casa, dado no provisionamento |
| `api_key` | credencial do agente, gerada no provisionamento |
| `cloud_url` | base das Edge Functions, termina em `/functions/v1` |
| `entity_allowlist` | prefixos a incluir; vazio = tudo menos a denylist |
| `entity_denylist` | prefixos a excluir de tudo, inclusive do inventario |

O `api_key` e um segredo: o Home Assistant guarda-o mascarado na
configuracao do add-on.
