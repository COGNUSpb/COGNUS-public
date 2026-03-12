#
# SPDX-License-Identifier: Apache-2.0
#
import io
import json
import tarfile
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


class ChaincodePackageUploadTests(APITestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="operador.upload",
            email="operador.upload@example.org",
            password="senha-segura-123",
            role="operator",
            organization=None,
        )
        self.client.force_authenticate(user=self.user)

        self.tempdir = TemporaryDirectory()
        self.chaincode_store_patcher = patch(
            "api.routes.chaincode.views.FABRIC_CHAINCODE_STORE",
            self.tempdir.name,
        )
        self.chaincode_store_patcher.start()

    def tearDown(self):
        self.chaincode_store_patcher.stop()
        self.tempdir.cleanup()
        super().tearDown()

    def _build_package_upload(self, file_name="reserve-book-cc_1.0.tar.gz"):
        metadata_payload = json.dumps(
            {
                "path": "github.com/example/reserve-book-cc",
                "type": "golang",
                "label": "reserve-book-cc_1.0",
            }
        ).encode("utf-8")

        code_buffer = io.BytesIO()
        with tarfile.open(fileobj=code_buffer, mode="w:gz"):
            pass
        code_payload = code_buffer.getvalue()

        package_buffer = io.BytesIO()
        with tarfile.open(fileobj=package_buffer, mode="w:gz") as package:
            metadata_info = tarfile.TarInfo("metadata.json")
            metadata_info.size = len(metadata_payload)
            package.addfile(metadata_info, io.BytesIO(metadata_payload))

            code_info = tarfile.TarInfo("code.tar.gz")
            code_info.size = len(code_payload)
            package.addfile(code_info, io.BytesIO(code_payload))

        package_buffer.seek(0)
        return SimpleUploadedFile(
            file_name,
            package_buffer.getvalue(),
            content_type="application/gzip",
        )

    def test_operator_without_organization_can_upload_chaincode_package(self):
        response = self.client.post(
            reverse("chaincode-package"),
            {
                "file": self._build_package_upload(),
                "description": "Provisioning wizard upload",
            },
            format="multipart",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "successful")
        self.assertTrue(response.data["data"]["artifact_ref"].startswith("local-file:"))
        self.assertEqual(response.data["data"]["file_name"], "reserve-book-cc_1.0.tar.gz")

        from api.models import ChainCode

        chaincode = ChainCode.objects.get(package_id=response.data["data"]["package_id"])
        self.assertEqual(chaincode.creator, "operador.upload")
