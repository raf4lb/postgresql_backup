# Backup de Banco de Dados PostgreSQL

Script para backup e upload remoto de banco de dados PostgreSQL.

## Funcionalidades
- Listagem de bancos de dados disponíveis;
- Backup do banco de dados;
- Upload do backup via SSH;

## Pré-requisitos
- Python 3;
- pysftp.

# Instalação
Instalar o módulo pysftp:
```
$ pip3 install pysftp
```
Editar o arquivo de configuração script.config:
```
[postgresql]
host=<host_do_banco>
port=<porta_do_banco>
db=<nome_do_banco>
user=<nome_do_usuario>
password=<senha_do_usuario>

[sftp]
host=<host_do_sftp>
user=<nome_do_usuario>
password=<senha>
log=<arquivo_de_log.log>
dest_folder=<pasta_para_salvar_o_backup>
```

## Rodando
Certifique-se de que o script.config esteja configurado.
Para listar os bancos de dados disponíveis para backup:
```
$ python3 postgresql_backup.py --configfile script.config --action list
```
Para fazer o backup do banco de dados configurado:
```
$ python3 postgresql_backup.py --configfile script.config --action backup
```
