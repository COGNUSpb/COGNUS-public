#
# SPDX-License-Identifier: Apache-2.0
#
from django.contrib import admin

# Registro dos modelos no admin
from .models import EventLog

@admin.register(EventLog)
class EventLogAdmin(admin.ModelAdmin):
	list_display = ("timestamp", "level", "message")
	list_filter = ("level", "timestamp")
	search_fields = ("message",)
