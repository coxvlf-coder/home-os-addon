# O contexto de build de um add-on e a pasta que contem o config.yaml.
ARG BUILD_FROM
FROM $BUILD_FROM

# `py3-yaml` vem do apk de proposito: o PyYAML tem extensao em C e
# compila-lo em musl/aarch64 exige toolchain completo. O pacote do Alpine
# ja vem compilado.
RUN apk add --no-cache python3 py3-pip py3-yaml

COPY requirements.txt /opt/

# As dependencias de compilacao entram num grupo virtual e saem a seguir:
# `httpx` e `websockets` podem nao ter wheel musl para esta arquitectura e
# aí precisam de compilar. Sem isto, a build falha com um erro que o
# Supervisor reporta apenas como "unknown error while trying to build the
# image" -- sem dizer o que faltou.
#
# `--break-system-packages` porque o Alpine marca o Python do sistema como
# externally-managed (PEP 668) e o pip recusa-se a instalar sem a flag.
# Dentro de um container dedicado nao ha ambiente de sistema a proteger.
RUN apk add --no-cache --virtual .build-deps gcc musl-dev python3-dev libffi-dev \
 && pip3 install --no-cache-dir --break-system-packages -r /opt/requirements.txt \
 && apk del .build-deps

COPY agent.py /opt/
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
