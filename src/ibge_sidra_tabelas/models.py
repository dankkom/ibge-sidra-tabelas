from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    ForeignKey,
    Identity,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SidraTabela(Base):
    __tablename__ = "sidra_tabela"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    periodicidade: Mapped[str] = mapped_column(Text, nullable=False)
    ultima_atualizacao: Mapped[Date] = mapped_column(Date, nullable=False)
    metadados: Mapped[JSONB] = mapped_column(JSONB, nullable=False)
    localidades: Mapped[JSONB] = mapped_column(JSONB, nullable=False)
    periodos: Mapped[JSONB] = mapped_column(JSONB, nullable=False)


class Localidade(Base):
    __tablename__ = "localidade"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    tabela_id: Mapped[str] = mapped_column(
        ForeignKey("sidra_tabela.id"),
        nullable=False,
        index=True,
    )
    # NC = NIVEL TERRITORIAL ID
    nc: Mapped[str] = mapped_column(Text, nullable=False)
    # NN = NIVEL TERRITORIAL NOME
    nn: Mapped[str] = mapped_column(Text, nullable=False)
    # D1C = UNIDADE TERRITORIAL ID
    d1c: Mapped[str] = mapped_column(Text, nullable=False)
    # D1N = UNIDADE TERRITORIAL NOME
    d1n: Mapped[str] = mapped_column(Text, nullable=False)


class Dimensao(Base):
    __tablename__ = "dimensao"
    __table_args__ = (
        UniqueConstraint(
            "mc",
            "d2c",
            "d4c",
            "d5c",
            "d6c",
            "d7c",
            "d8c",
            "d9c",
            name="uq_dimensao",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    sidra_tabela_id: Mapped[str] = mapped_column(
        ForeignKey("sidra_tabela.id"),
        nullable=False,
        index=True,
    )
    # MC = UNIDADE ID
    mc: Mapped[str] = mapped_column(Text, nullable=False)
    # MN = UNIDADE NOME
    mn: Mapped[str] = mapped_column(Text, nullable=False)
    # D2C = VARIAVEL ID
    d2c: Mapped[str] = mapped_column(Text, nullable=False)
    # D2N = VARIAVEL NOME
    d2n: Mapped[str] = mapped_column(Text, nullable=False)
    # D4C = CATEGORIA ID da Classificação 1
    d4c: Mapped[str] = mapped_column(Text, nullable=True)
    # D4N = CATEGORIA NOME da Classificação 1
    d4n: Mapped[str] = mapped_column(Text, nullable=True)
    # D5C = CATEGORIA ID da Classificação 2
    d5c: Mapped[str] = mapped_column(Text, nullable=True)
    # D5N = CATEGORIA NOME da Classificação 2
    d5n: Mapped[str] = mapped_column(Text, nullable=True)
    # D6C = CATEGORIA ID da Classificação 3
    d6c: Mapped[str] = mapped_column(Text, nullable=True)
    # D6N = CATEGORIA NOME da Classificação 3
    d6n: Mapped[str] = mapped_column(Text, nullable=True)
    # D7C = CATEGORIA ID da Classificação 4
    d7c: Mapped[str] = mapped_column(Text, nullable=True)
    # D7N = CATEGORIA NOME da Classificação 4
    d7n: Mapped[str] = mapped_column(Text, nullable=True)
    # D8C = CATEGORIA ID da Classificação 5
    d8c: Mapped[str] = mapped_column(Text, nullable=True)
    # D8N = CATEGORIA NOME da Classificação 5
    d8n: Mapped[str] = mapped_column(Text, nullable=True)
    # D9C = CATEGORIA ID da Classificação 6
    d9c: Mapped[str] = mapped_column(Text, nullable=True)
    # D9N = CATEGORIA NOME da Classificação 6
    d9n: Mapped[str] = mapped_column(Text, nullable=True)


class Dados(Base):
    __tablename__ = "dados"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    sidra_tabela_id: Mapped[str] = mapped_column(
        ForeignKey("sidra_tabela.id"),
        nullable=False,
        index=True,
    )
    dimensao_id: Mapped[int] = mapped_column(
        ForeignKey("dimensao.id"),
        nullable=False,
        index=True,
    )
    # D3C = PERIODO ID
    d3c: Mapped[str] = mapped_column(Text, nullable=False)
    modificacao: Mapped[Date] = mapped_column(Date, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # VALOR
    v: Mapped[str] = mapped_column(Text, nullable=False)
