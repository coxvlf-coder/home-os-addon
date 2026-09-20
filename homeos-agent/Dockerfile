# O contexto de build de um add-on e a pasta que contem o config.yaml --
# por isso o add-on E a pasta `edge-agent/`, e nao uma subpasta dela: um
# Dockerfile nao consegue copiar de fora do seu contexto.
ARG BUILD_FROM
FROM $BUILD_FROM

# `--no-cache` em vez de apk update + rm: uma camada a menos e sem indice
# de pacotes a envelhecer dentro da imagem.
RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /opt/
# Alpine marca o Python do sistema como externally-managed (PEP 668) e o
# pip recusa-se a instalar sem isto. Dentro de um container dedicado nao
# ha ambiente de sistema a proteger.
RUN pip3 install --no-cache-dir --break-system-packages -r /opt/requirements.txt

COPY agent.py /opt/
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
