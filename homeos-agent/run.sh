#!/usr/bin/with-contenv bashio
# O agente nao vai para background: o Supervisor trata do ciclo de vida e
# reinicia o container se o processo morrer. Um daemon aqui dentro
# esconderia falhas de arranque atras de um container "a correr".
bashio::log.info "Home OS agent a arrancar"
exec python3 /opt/agent.py
