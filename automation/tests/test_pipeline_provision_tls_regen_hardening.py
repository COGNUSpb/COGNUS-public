import unittest
from automation.pipeline_provision import _tls_regenerate_with_san_command


class TlsRegenHardeningTest(unittest.TestCase):
    def test_regen_command_includes_multi_san_and_passin_and_serial(self):
        cert = "/tmp/out/cert.pem"
        key = "/tmp/out/key.pem"
        ca_cert = "/var/cognus/crypto/ca/tlsca.crt"
        ca_key = "/var/cognus/crypto/ca/tlsca.key"
        san_list = ["example.com", "127.0.0.1"]
        cmd = _tls_regenerate_with_san_command(cert, key, ca_cert, ca_key, san_list=san_list)
        # basic checks on generated shell command
        self.assertIn("subjectAltName=DNS:example.com,IP:127.0.0.1", cmd)
        self.assertIn("CA_KEY_PASSPHRASE", cmd)
        self.assertTrue(("CAcreateserial" in cmd) or ("CAserial" in cmd))
        self.assertIn("openssl genrsa", cmd)
        self.assertIn(cert, cmd)


if __name__ == "__main__":
    unittest.main()
