from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    ForeignKey,
    Identity,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SidraTabela(Base):
    __tablename__ = "sidra_tabela"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    periodicidade: Mapped[str] = mapped_column(Text, nullable=False)
    ultima_atualizacao: Mapped[Date] = mapped_column(Date, nullable=False)


class SidraMetadados(Base):
    __tablename__ = "sidra_metadados"
    __table_args__ = (
        UniqueConstraint(
            "tabela_id",
            "nc",
            "mc",
            "d1c",
            "d2c",
            "d4c",
            "d5c",
            "d6c",
            "d7c",
            "d8c",
            "d9c",
            name="uq_sidra_metadados_dimensions",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    tabela_id: Mapped[str] = mapped_column(
        ForeignKey("sidra_tabela.id"), nullable=False
    )
    # NC = NIVEL TERRITORIAL ID
    nc: Mapped[str] = mapped_column(Text, nullable=False)
    # MC = UNIDADE ID
    mc: Mapped[str] = mapped_column(Text, nullable=False)
    # D1C = UNIDADE TERRITORIAL ID
    d1c: Mapped[str] = mapped_column(Text, nullable=False)
    # D2C = VARIAVEL ID
    d2c: Mapped[str] = mapped_column(Text, nullable=False)
    d4c: Mapped[str] = mapped_column(Text, nullable=True)
    d5c: Mapped[str] = mapped_column(Text, nullable=True)
    d6c: Mapped[str] = mapped_column(Text, nullable=True)
    d7c: Mapped[str] = mapped_column(Text, nullable=True)
    d8c: Mapped[str] = mapped_column(Text, nullable=True)
    d9c: Mapped[str] = mapped_column(Text, nullable=True)


class SidraDados(Base):
    __tablename__ = "sidra_dados"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )
    sidra_metadados_id: Mapped[str] = mapped_column(
        ForeignKey("sidra_metadados.id"), nullable=False
    )
    # D3C = PERIODO ID
    d3c: Mapped[str] = mapped_column(Text, nullable=False)
    modificacao: Mapped[Date] = mapped_column(Date, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # VALOR
    v: Mapped[str] = mapped_column(Text, nullable=False)
