FROM ubuntu:25.04

# Pacotes básicos
RUN apt-get update && apt-get install -y \
  curl iproute2 ssh openvpn git docker.io docker-compose \
  && apt-get clean

# Diretório de trabalho
WORKDIR /home/project

# Copia o projeto G7 e o arquivo de exclusão
COPY finalproject/20251/g7 ./g7/
COPY finalproject/20251/g7/misc/exclude.txt ./exclude.txt

# Copia o arquivo .ovpn
COPY vmaccess/ovpns/pdm-vpn.ovpn ./pdm-vpn.ovpn

# Torna scripts executáveis
RUN chmod +x ./g7/bin/*.sh

# Define o diretório padrão e a execução
WORKDIR /home/project/g7
CMD ["bash", "bin/vpn-and-ssh-to-host-docker.sh"]
