#
# SPDX-License-Identifier: Apache-2.0
#

from .serializers import (
    LoginBody,
    LoginSuccessBody,
    TokenVerifyRequest,
)
from api.common import ok, err

from api.routes.general.serializers import (
    RegisterBody,
    RegisterResponse,
)
from api.models import UserProfile
from rest_framework.response import Response
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
import logging

from django.contrib.auth import authenticate
from rest_framework import viewsets, status
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenVerifyView,
)
from rest_framework_simplejwt.tokens import (
    RefreshToken,
    AccessToken,
)
from rest_framework_simplejwt.exceptions import TokenError
from django.db.models import Q

LOG = logging.getLogger(__name__)


class RegisterViewSet(viewsets.ViewSet):
    def create(self, request):
        try:
            serializer = RegisterBody(data=request.data)
            if serializer.is_valid(raise_exception=True):
                email = serializer.validated_data.get("email")
                username = serializer.validated_data.get("username")
                password = serializer.validated_data.get("password")

                if UserProfile.objects.filter(
                    Q(email=email) | Q(username=username)
                ).exists():
                    return Response(
                        err("Account already exists!"),
                        status=status.HTTP_409_CONFLICT,
                    )

                user = UserProfile(
                    username=username,
                    email=email,
                    role="operator",
                    organization=None,
                )
                user.set_password(password)
                user.save()

                response = RegisterResponse(data={"id": user.id})
                if response.is_valid(raise_exception=True):
                    return Response(
                        data=ok(response.validated_data),
                        status=status.HTTP_200_OK,
                    )
        except Exception as e:
            return Response(err(e.args), status=status.HTTP_400_BAD_REQUEST)


class CelloTokenObtainPairView(TokenObtainPairView):
    def post(self, request, *args, **kwargs):
        serializer = LoginBody(data=request.data)
        if serializer.is_valid(raise_exception=True):
            email = serializer.validated_data["email"]
            password = serializer.validated_data["password"]
            try:
                user = authenticate(
                    request,
                    username=email,
                    password=password,
                )
            except MultipleObjectsReturned:
                user = (
                    UserProfile.objects.filter(
                        Q(username=email) | Q(email=email)
                    )
                    .order_by("id")
                    .first()
                )
                if user is None or not user.check_password(password):
                    user = None
            if user is not None:
                refresh = RefreshToken.for_user(user)
                data = {
                    "token": str(refresh.access_token),
                    "user": user,
                }
                response = LoginSuccessBody(instance=data)
                return Response(
                    data=ok(response.data),
                    status=200,
                )
        return super().post(request, *args, **kwargs)


class CelloTokenVerifyView(TokenVerifyView):
    def post(self, request, *args, **kwargs):
        serializer = TokenVerifyRequest(data=request.data)
        if serializer.is_valid(raise_exception=True):
            try:
                access_token = AccessToken(
                    token=serializer.validated_data["token"],
                )
                user = UserProfile.objects.get(pk=access_token["user_id"])
                if user is not None:
                    data = {
                        "token": str(access_token.token),
                        "user": user,
                    }
                    response = LoginSuccessBody(instance=data)
                    return Response(
                        data=ok(response.data),
                        status=200,
                    )
            except TokenError:
                LOG.exception("invalid token error")
                return Response(data=err(msg="invalid token"), status=401)

        return super().post(request, *args, **kwargs)
