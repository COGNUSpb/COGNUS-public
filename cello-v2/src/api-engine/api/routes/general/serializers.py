#
# SPDX-License-Identifier: Apache-2.0

from api.routes.user.serializers import UserInfoSerializer
from rest_framework import serializers


class RegisterBody(serializers.Serializer):
    username = serializers.CharField(help_text="name of Operator")
    email = serializers.EmailField(help_text="email of user")
    password = serializers.CharField(help_text="password of Operator")


class RegisterIDSerializer(serializers.Serializer):
    id = serializers.UUIDField(help_text="ID of User")


class RegisterResponse(serializers.Serializer):
    id = serializers.UUIDField(help_text="ID of User")


class LoginBody(serializers.Serializer):
    email = serializers.CharField(help_text="email of user")
    password = serializers.CharField(help_text="password of user")


class LoginSuccessBody(serializers.Serializer):
    token = serializers.CharField(help_text="access token")
    user = UserInfoSerializer()


class TokenVerifyRequest(serializers.Serializer):
    token = serializers.CharField(help_text="access token")
