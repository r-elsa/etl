import os

class Config():
    TESTING = False
    SQLALCHEMY_DATABASE_URI = 'postgres://localhost/flask-heroku'

class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")

class DevelopmentConfig(Config):
    pass

class TestingConfig(Config):
    TESTING = True