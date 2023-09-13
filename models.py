from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

Base = declarative_base()


class CrawlerInfo(Base):
    __tablename__ = "crawler_data"

    id = sa.Column(sa.INTEGER, primary_key=True)
    link = sa.Column(sa.TEXT)
    crawled = sa.Column(sa.BOOLEAN)

