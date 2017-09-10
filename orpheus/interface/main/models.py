# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# Create your models here.

class CVDs(models.Model):
	name = models.CharField(max_length=200)

class PrivateFiles(models.Model):
	name = models.CharField(max_length=200)

class PrivateTables(models.Model):
	name = models.CharField(max_length=200)