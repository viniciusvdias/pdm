#!/bin/bash
# Script para rodar o projeto
docker-compose -f misc/docker-compose.yml build
docker-compose -f misc/docker-compose.yml up