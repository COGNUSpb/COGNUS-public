#!/usr/bin/env python3
"""Place an identity file into a gateway container wallet and restart the container.

Usage example:
  python3 scripts/provision/fix_gateway_identity.py \
    --container cognusrb-cr-...-apigat \
    --cert /var/cognus/crypto/org-inf13/apigateway-org-inf13/msp/signcerts/admin.pem \
    --key /var/cognus/crypto/org-inf13/apigateway-org-inf13/msp/keystore/admin_key.pem \
    --wallet-path /app/.gateway-wallet \
    --id org-inf13-admin

This script writes a file named `<id>.id` containing the certificate followed by the private key
into the gateway wallet directory and restarts the container so the gateway picks up the identity.
"""
import argparse
import sys
import logging
from docker import from_env


def read_file(path):
    with open(path, 'r') as f:
        return f.read()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--container', required=True, help='Gateway container name or id')
    parser.add_argument('--cert', required=True, help='Path to admin certificate (PEM) on host')
    parser.add_argument('--key', required=True, help='Path to admin private key (PEM) on host')
    parser.add_argument('--wallet-path', default='/app/.gateway-wallet', help='Wallet path inside container')
    parser.add_argument('--id', default='org-admin', help='Identity filename (without extension)')
    parser.add_argument('--restart', action='store_true', help='Restart container after writing identity')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    try:
        cert_pem = read_file(args.cert)
    except Exception as e:
        logging.error('Failed to read cert %s: %s', args.cert, e)
        sys.exit(2)
    try:
        key_pem = read_file(args.key)
    except Exception as e:
        logging.error('Failed to read key %s: %s', args.key, e)
        sys.exit(2)

    wallet_content = cert_pem.strip() + '\n' + key_pem.strip() + '\n'

    docker = from_env()
    try:
        container = docker.containers.get(args.container)
    except Exception as e:
        logging.error('Cannot find container %s: %s', args.container, e)
        sys.exit(3)

    target_path = args.wallet_path.rstrip('/') + '/' + args.id + '.id'

    # Ensure wallet dir exists and write file via shell redirection (stdin)
    mkdir_cmd = f"mkdir -p {args.wallet_path} && chmod 700 {args.wallet_path} || true"
    logging.info('Ensuring wallet dir exists inside container: %s', args.wallet_path)
    rc = container.exec_run(['/bin/sh', '-c', mkdir_cmd])
    if rc.exit_code != 0:
        logging.warning('mkdir returned non-zero exit code: %s', rc.exit_code)

    write_cmd = f"sh -c 'cat > {target_path} && chmod 600 {target_path}'"
    logging.info('Writing identity file to %s inside container %s', target_path, args.container)
    try:
        # Try socket-style exec (newer docker SDK)
        exec_res = container.exec_run(write_cmd, stdin=True, socket=True)
        # If this returns without error we need to send input via the socket - fallback to input mode below
        # Older docker-py versions allow input parameter directly
    except TypeError:
        # older docker-py returns tuple-like result; use exec_run with input
        res = container.exec_run(write_cmd, stdin=True, input=wallet_content.encode('utf-8'))
        if res.exit_code != 0:
            logging.error('Writing identity failed: exit_code=%s output=%s', res.exit_code, res.output)
            sys.exit(4)
    except Exception:
        # try the input style
        res = container.exec_run(write_cmd, stdin=True, input=wallet_content.encode('utf-8'))
        if res.exit_code != 0:
            logging.error('Writing identity failed: exit_code=%s output=%s', res.exit_code, res.output)
            sys.exit(4)

    # verify file exists
    ls_res = container.exec_run(['/bin/ls', '-l', target_path])
    logging.info('Verification: ls -l %s -> %s', target_path, ls_res.output.decode('utf-8', errors='ignore').strip())

    if args.restart:
        logging.info('Restarting container %s', args.container)
        try:
            container.restart()
        except Exception as e:
            logging.error('Failed to restart container: %s', e)
            sys.exit(5)

    logging.info('Identity %s installed into %s', args.id, args.container)


if __name__ == '__main__':
    main()
