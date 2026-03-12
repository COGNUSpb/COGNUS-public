import os

from api.models import UserProfile


ADMIN_EMAIL = str(os.getenv("API_ENGINE_ADMIN_EMAIL", "") or "").strip()
ADMIN_PASSWORD = str(os.getenv("API_ENGINE_ADMIN_PASSWORD", "") or "").strip()
ADMIN_USERNAME = str(
    os.getenv("API_ENGINE_ADMIN_USERNAME", os.getenv("ADMIN_USERNAME", "")) or ""
).strip()

if not ADMIN_EMAIL or not ADMIN_PASSWORD or not ADMIN_USERNAME:
    print(
        "Bootstrap admin desabilitado: defina API_ENGINE_ADMIN_EMAIL, "
        "API_ENGINE_ADMIN_USERNAME e API_ENGINE_ADMIN_PASSWORD em ambiente privado."
    )
elif not UserProfile.objects.filter(email=ADMIN_EMAIL).exists():
    UserProfile.objects.create_superuser(
        email=ADMIN_EMAIL,
        username=ADMIN_USERNAME,
        password=ADMIN_PASSWORD,
    )
    print("Superusuário bootstrap criado com sucesso.")
else:
    print("Superusuário bootstrap já existe.")
