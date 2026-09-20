#!/bin/sh
# O AMBIENTE DO s6 RECUPERADO EM SH PURO.
#
# Historia desta linha, porque ela ja custou duas versoes:
#
#   0.2.1  `#!/usr/bin/with-contenv bashio` -- o container morria antes da
#          primeira linha, log completamente vazio. O `bashio` nao existe
#          nesta imagem: vem nas bases `-base-python`/`-base-debian`, e o
#          Dockerfile daqui instala python sobre a base nua.
#   0.2.2  `#!/bin/sh` -- arrancou, mas CEGO. Sob s6-overlay v3 o `/init`
#          retira o ambiente do container antes de executar o CMD e
#          guarda-o em /run/s6/container_environment/. Sem `with-contenv`
#          o processo nao ve variavel nenhuma -- nem SUPERVISOR_TOKEN,
#          nem nada. Confirmado no log: "variaveis relevantes: NENHUMA".
#
# A saida obvia era `#!/usr/bin/with-contenv sh`, que existe nesta imagem
# por ser parte do s6 e nao do bashio. Mas isso volta a pendurar o
# arranque num binario cuja presenca nao controlamos -- e a falha dele e
# a pior de todas, um container morto sem uma linha de log.
#
# Em vez disso, faz-se o que o `with-contenv` faz: ler o directorio. Cada
# ficheiro la dentro e uma variavel, o nome e o nome e o conteudo e o
# valor. Funciona com ou sem o binario, e se o directorio nao existir
# (a correr fora de um add-on) simplesmente nao faz nada.
if [ -d /run/s6/container_environment ]; then
  for f in /run/s6/container_environment/*; do
    [ -f "$f" ] && export "$(basename "$f")=$(cat "$f")"
  done
  echo "[homeos] ambiente do s6 recuperado"
fi

echo "[homeos] arranque"
python3 --version
[ -f /data/options.json ] && echo "[homeos] configuracao encontrada" \
                          || echo "[homeos] AVISO: /data/options.json nao existe"

exec python3 -u /opt/agent.py
