"""Backward compatibility shim."""
from core.foundation.catalog import *
from core.foundation.catalog import hooks, tracing, webhooks
# yaml_generator moved to domain layer
from core.domain.catalog import yaml_generator
