#!/usr/bin/python3
import argparse
import logging
import subprocess
import os

import configparser
import gzip
import pysftp

import datetime

BACKUP_PATH = '/tmp/'


def upload_to_server(host, username, password, logfile, dest_folder, file_full_path, remove_after_upload=False):
    """
    Upload a file to a server via sftp.
    """
    try:
        with pysftp.Connection(
            host=host, 
            username=username,
            password=password,
            log=logfile,
        ) as sftp:
            sftp.cwd(dest_folder) # The full path
            sftp.put(file_full_path) # Upload the file
            if remove_after_upload:
                os.remove(file_full_path)
    except Exception as exc:
        print(exc)
        exit(1)


def list_postgres_databases(host, database_name, port, user, password):
    """
    List available databases.
    """
    try:
        process = subprocess.Popen(
            ['psql',
             '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
             '--list'],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
            exit(1)
        return output
    except Exception as e:
        print(e)
        exit(1)


def backup_postgres_db(host, database_name, port, user, password, dest_file, verbose):
    """
    Backup postgres db to a file.
    """
    if verbose:
        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-Fc',
                 '-f', dest_file,
                 '-v'],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)
    else:
        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-f', dest_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)


def compress_file(src_file):
    compressed_file = "{}.gz".format(str(src_file))
    with open(src_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return compressed_file


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args_parser = argparse.ArgumentParser(description='Postgres database management')
    args_parser.add_argument("--action",
                             metavar="action",
                             choices=['list', 'backup'],
                             required=True)
    args_parser.add_argument("--verbose",
                             default=True,
                             help="verbose output")
    args_parser.add_argument("--configfile",
                             required=True,
                             help="Database configuration file")
    args = args_parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.configfile)

    postgres_host = config.get('postgresql', 'host')
    postgres_port = config.get('postgresql', 'port')
    postgres_db = config.get('postgresql', 'db')
    postgres_restore = "{}_restore".format(postgres_db)
    postgres_user = config.get('postgresql', 'user')
    postgres_password = config.get('postgresql', 'password')
    sftp_host = config.get('sftp', 'host')
    sftp_user = config.get('sftp', 'user')
    sftp_password = config.get('sftp', 'password')
    sftp_log = config.get('sftp', 'log')
    sftp_dest_folder = config.get('sftp', 'dest_folder')
    timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = 'backup-{}-{}.dump'.format(timestr, postgres_db)
    filename_compressed = '{}.gz'.format(filename)
    local_file_path = '{}{}'.format(BACKUP_PATH, filename)

    # list databases task
    if args.action == "list":
        result = list_postgres_databases(
            postgres_host,
            postgres_db,
            postgres_port,
            postgres_user,
            postgres_password,
        )
        for line in result.splitlines():
            logger.info(line)
    # backup task
    elif args.action == "backup":
        logger.info('Backing up {} database to {}'.format(postgres_db, local_file_path))
        result = backup_postgres_db(
            postgres_host,
            postgres_db,
            postgres_port,
            postgres_user,
            postgres_password,
            local_file_path,
            args.verbose,
        )
        for line in result.splitlines():
            logger.info(line)

        logger.info("Backup complete")
        logger.info("Compressing {}".format(local_file_path))
        comp_file = compress_file(local_file_path)
        logger.info('Uploading {} to sftp server...'.format(comp_file))
        upload_to_server(sftp_host, sftp_user, sftp_password, sftp_log, sftp_dest_folder, comp_file)
        logger.info("Uploaded to {}".format(filename_compressed))
    else:
        logger.warn("No valid argument was given.")
        logger.warn(args)


if __name__ == '__main__':
    main()
