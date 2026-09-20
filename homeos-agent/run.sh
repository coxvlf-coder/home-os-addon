#!/bin/sh
# SH PURO, SEM BASHIO, E COM PYTHON SEM BUFFER.
#
# A versao anterior comecava com `#!/usr/bin/with-contenv bashio`, que e o
# padrao dos add-ons da comunidade mas depende de o bashio existir na
# imagem e do s6-overlay estar activo. Com `init: false` o Supervisor corre
# o CMD directamente -- e se o interpretador do shebang nao existir, o
# container morre ANTES da primeira linha e o log fica vazio, que foi
# exactamente o que aconteceu.
#
# O `-u` e a outra metade: sem ele o Python bufferiza o stdout quando nao
# esta num terminal, e o registo do add-on fica em branco mesmo com o
# agente a correr. Um log vazio deixa de poder significar duas coisas
# diferentes.
echo "[homeos] arranque"
python3 --version
if [ -f /data/options.json ]; then
  echo "[homeos] configuracao encontrada em /data/options.json"
else
  echo "[homeos] AVISO: /data/options.json nao existe"
fi
exec python3 -u /opt/agent.py
