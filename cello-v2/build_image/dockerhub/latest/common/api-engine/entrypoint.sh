#!/usr/bin/env bash

#bash /scripts/initial.sh;

echo "Generating the settings.py for api_engine"
LOCAL_SETTINGS="/var/www/server/api_engine/settings.py"
RAW_LOCAL_SETTINGS="/var/www/server/api_engine/settings.py.example"

envsubst < ${RAW_LOCAL_SETTINGS} > ${LOCAL_SETTINGS}

holdup -t 120 tcp://${DB_HOST}:${DB_PORT};
if [[ "$RUN_MODE" == "server" ]]; then
  python manage.py migrate;
  BOOTSTRAP_ADMIN_FLAG="${API_ENGINE_BOOTSTRAP_ADMIN:-${BOOTSTRAP_ADMIN:-false}}"
  ADMIN_BOOTSTRAP_USERNAME="${API_ENGINE_ADMIN_USERNAME:-${ADMIN_USERNAME:-}}"
  ADMIN_BOOTSTRAP_PASSWORD="${API_ENGINE_ADMIN_PASSWORD:-${ADMIN_PASSWORD:-}}"
  ADMIN_BOOTSTRAP_EMAIL="${API_ENGINE_ADMIN_EMAIL:-${ADMIN_EMAIL:-}}"

  if [[ "${BOOTSTRAP_ADMIN_FLAG,,}" == "true" || "${BOOTSTRAP_ADMIN_FLAG}" == "1" ]]; then
    if [[ -z "${ADMIN_BOOTSTRAP_USERNAME}" || -z "${ADMIN_BOOTSTRAP_PASSWORD}" || -z "${ADMIN_BOOTSTRAP_EMAIL}" ]]; then
      echo "Skipping admin bootstrap: set API_ENGINE_ADMIN_USERNAME, API_ENGINE_ADMIN_PASSWORD and API_ENGINE_ADMIN_EMAIL when API_ENGINE_BOOTSTRAP_ADMIN=true.";
    else
      python manage.py create_user \
        --username "${ADMIN_BOOTSTRAP_USERNAME}" \
        --password "${ADMIN_BOOTSTRAP_PASSWORD}" \
        --email "${ADMIN_BOOTSTRAP_EMAIL}" \
        --is_superuser \
        --role admin
    fi
  else
    echo "Admin bootstrap disabled for this deployment.";
  fi
  if [[ "$DEBUG" == "True" ]]; then # For dev, use pure Django directly
    python manage.py runserver 0.0.0.0:8080;
  else # For production, use uwsgi in front
    uwsgi --ini /etc/uwsgi/apps-enabled/server.ini;
  fi
else
  celery -A api_engine worker -l info
fi
