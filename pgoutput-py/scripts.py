import click
import psycopg2
from consumer import *


@click.group()
def pgoutput_py():
    print("Hello from pgoutput-py")


@click.command()
@click.option("--host", default="localhost", help="PostgreSQL host")
@click.option("--port", default=6432, help="PostgreSQL port")
@click.option("--user", default="gorgias", help="PostgreSQL user")
@click.option("--password", default="gorgias", help="PostgreSQL password")
@click.option("--database", default="gorgias", help="PostgreSQL database")
@click.option("--slot", default="test_slot", help="PostgreSQL replication slot")
@click.option("--publication", default="test_pub", help="PostgreSQL publication")
@click.option("--n", default=5, help="Number of messages to consume")
@click.option("--peek", is_flag=True, help="Peek at the next message without acking it")
def advance(host, port, user, password, database, slot, publication, n, peek):
    connection = psycopg2.connect(
        f"host={host} port={port} user={user} password={password} dbname={database}",
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    cursor = connection.cursor()
    cursor.start_replication(
        slot_name=slot, decode=False, options={"proto_version": 1, "publication_names": publication}
    )
    cursor.consume_stream(FiniteConsumer(n))


pgoutput_py.add_command(advance, name="advance")
