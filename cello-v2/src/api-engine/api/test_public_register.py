#
# SPDX-License-Identifier: Apache-2.0
#
from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


class PublicRegisterTests(APITestCase):
    def test_public_register_creates_operator_without_organization(self):
        response = self.client.post(
            reverse("register-list"),
            {
                "username": "operador.publico",
                "email": "operador.publico@example.org",
                "password": "senha-segura-123",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "successful")

        user_model = get_user_model()
        user = user_model.objects.get(email="operador.publico@example.org")
        self.assertEqual(user.username, "operador.publico")
        self.assertEqual(user.role, "operator")
        self.assertIsNone(user.organization)

    def test_public_register_rejects_duplicate_email_or_username(self):
        user_model = get_user_model()
        user_model.objects.create_user(
            username="operador.duplicado",
            email="operador.duplicado@example.org",
            password="senha-segura-123",
            role="operator",
            organization=None,
        )

        response = self.client.post(
            reverse("register-list"),
            {
                "username": "operador.duplicado",
                "email": "operador.duplicado@example.org",
                "password": "senha-segura-123",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["status"], "fail")
