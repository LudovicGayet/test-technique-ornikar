import click
import os
from typing import List, Iterable
import datetime
from abc import ABC, abstractmethod

from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel

GCP_PROJECT = "test-data-engineer-090621"
DATE_FORMAT = "%Y-%m-%d"


class PartenariatInformationRepository(ABC):
    """
    Classe abstraite pour la gestion de la source de donnée qui contient les informations de partenariat
    """

    @abstractmethod
    def compute_nombre_lecon_par_departement(
        self, type_partenariat: str, date_debut: str, date_fin: str
    ) -> Iterable:
        """calcul le nombre de lecon par departement pour un partenariat et une plage de temps donnée

        Args:
            type_partenariat (str): type de partenariat
            date_debut (str): date_debut
            date_fin (str): date_fin

        Returns:
            Iterable: iterable contenant un dictionnaire par partenariat et departement
        """
        pass


class Partenariat(BaseModel):
    type: str

    def get_nombre_lecon_par_departement(
        self,
        date_debut: str,
        date_fin: str,
        repository: PartenariatInformationRepository,
    ) -> Iterable:
        """calcul le nombre de lecon par departement pour ce partenariat sur une plage de temps donnée

        Args:
            date_debut (str): date_debut
            date_fin (str): date_fin
            repository (PartenariatInformationRepository): source de donnée qui contient les informations de partenariat

        Returns:
            Iterable: iterable contenant un dictionnaire par partenariat et departement
        """
        return repository.compute_nombre_lecon_par_departement(
            type_partenariat=self.type, date_debut=date_debut, date_fin=date_fin
        )


class InMemoryPartenariatInformationRepository(PartenariatInformationRepository):
    """
    Implémentation local de la DB qui contient les informations de partenariat, utile pour les tests
    """

    def __init__(self, *args, **kwargs):
        self.data = [
            {"partnership_type": "EIRL", "departement": 70, "number_of_lessons": 1},
            {"partnership_type": "EIRL", "departement": 71, "number_of_lessons": 2},
            {"partnership_type": "EI", "departement": 72, "number_of_lessons": 3},
            {"partnership_type": "EI", "departement": 73, "number_of_lessons": 4},
            {"partnership_type": "EI", "departement": 74, "number_of_lessons": 5},
            {"partnership_type": "EI", "departement": 75, "number_of_lessons": 6},
        ]

    def compute_nombre_lecon_par_departement(
        self, type_partenariat: str, date_debut: str, date_fin: str
    ) -> Iterable:
        for dictionnary in self.data:
            if dictionnary.get("partnership_type") == type_partenariat:
                yield dictionnary


class BigQueryPartenariatInformationRepository(PartenariatInformationRepository):
    """
    Implémentation Bigquery de la DB qui contient les informations de partenariat
    """

    def __init__(self, bigquery_client: bigquery.Client):
        self.bigquery_client = bigquery_client

    def compute_nombre_lecon_par_departement(
        self, type_partenariat: str, date_debut: str, date_fin: str
    ) -> Iterable:
        query_job = self.bigquery_client.query(
            f"""
                WITH extract_lecon_effectuees AS (
                    SELECT lessons.*
                    FROM test_dataset.lessons AS lessons
                    -- pas de left join pour ignorer toute leçon sans booking
                    INNER JOIN test_dataset.bookings AS bookings
                        USING(lesson_id)
                    WHERE
                        -- La leçon ne doit pas avoir été supprimée, ou bien après que la leçon fût terminé et on considère donc que la leçon a eut lieu malgré la suppression de la leçon
                        (
                            lessons.lesson_deleted_at IS NULL
                            OR (lessons.lesson_start_at < lessons.lesson_deleted_at)
                        )
                        -- Le Booking ne doit pas avoir été supprimé, ou bien après que la leçon fût terminé et on considère que la leçon a eut lieu malgré la suppression du booking
                        AND (
                            bookings.booking_deleted_at IS NULL
                            OR (bookings.booking_deleted_at > lessons.lesson_start_at)
                        )
                        -- Le Booking doit avoir été créé avant que l'heure de la leçon début
                        AND (bookings.booking_created_at <= lessons.lesson_start_at)
                ),

                calcul_nombre_lecon_par_departement_selon_type_partenariat AS (
                    SELECT
                        instructors.partnership_type,
                        meeting_points.departement,
                        count(*) as number_of_lessons,
                    FROM extract_lecon_effectuees
                    LEFT JOIN (
                        SELECT
                            meeting_points.*,
                            CAST(ROUND(75 + RAND() * (75 - 80)) AS INT64) AS departement
                        FROM `test_dataset.meeting_points` AS meeting_points
                    ) AS meeting_points
                        USING (meeting_point_id)
                    LEFT JOIN `test_dataset.instructors` AS instructors
                        USING (instructor_id)
                    WHERE date(lesson_start_at) BETWEEN DATE("{date_debut}") AND DATE("{date_fin}")
                        AND partnership_type = "{type_partenariat}"
                    GROUP BY partnership_type, departement
                )

                SELECT
                    *
                FROM calcul_nombre_lecon_par_departement_selon_type_partenariat
                ORDER BY partnership_type, departement
            """
        )

        results = query_job.result()

        for result in results:
            yield dict(result)


@click.command()
@click.option(
    "--partnership_type",
    "-p",
    required=True,
    type=str,
    multiple=True,
    help="Limit response number of the request",
)
@click.option(
    "--date_debut",
    "-d",
    required=True,
    type=str,
    help="Date debut au format {DATE_FORMAT}",
)
@click.option(
    "--date_fin",
    "-f",
    required=True,
    type=str,
    help=f"Date fin au format {DATE_FORMAT})",
)
def cli(
    partnership_type: List[str],
    date_debut: str,
    date_fin: str,
):
    f"""CLI permettant de récupérer le nombre de leçons par département en fonction du type de partenariat

    Args:
        partnership_type (List[str]): Limit response number of the request
        start_date (str): Date debut au format {DATE_FORMAT}
        end_date (str): Date fin au format {DATE_FORMAT}
    """
    try:
        date_debut = datetime.datetime.strptime(date_debut, DATE_FORMAT)
        date_fin = datetime.datetime.strptime(date_fin, DATE_FORMAT)
    except ValueError as exception:
        raise Exception(f"Mauvais format de date détecté => {exception}")

    partenariat_repository = BigQueryPartenariatInformationRepository(
        bigquery.Client(
            project=GCP_PROJECT,
            credentials=service_account.Credentials.from_service_account_file(
                os.path.dirname(os.path.realpath(__file__))
                + "/../Credentials/gcp-bigquery-credentials.json"
            ),
        )
    )

    results = list()
    for type in partnership_type:
        partenariat = Partenariat(type=type)
        iterator = partenariat.get_nombre_lecon_par_departement(
            date_debut=date_debut, date_fin=date_fin, repository=partenariat_repository
        )
        for dictionnary in iterator:
            results.append(dictionnary)
    print(results)
    return results


if __name__ == "__main__":
    cli()
