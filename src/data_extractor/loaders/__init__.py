"""Loader subpackage — imports trigger @register_loader decorators."""

from data_extractor.loaders.json_local import JSONLocalLoader  # noqa: F401
from data_extractor.loaders.sqlalchemy_loader import SQLAlchemyLoader  # noqa: F401
