from click.testing import CliRunner
from unittest.mock import patch
from question_b import cli, InMemoryPartenariatInformationRepository
import pytest


@patch(
    "question_b.BigQueryPartenariatInformationRepository",
    InMemoryPartenariatInformationRepository,
)
@pytest.mark.parametrize(
    "args,exit_code, output, exception",
    [
        (["-p", "EI", "-d", "2020-02-01", "-f", "2020-09-01"], 0, None, None),
        (
            ["-p", "EI", "-p", "EIRL", "-d", "2020-02-01", "-f", "2020-09-01"],
            0,
            None,
            None,
        ),
        ([], 2, "Missing option '--partnership_type' / '-p'", None),
        (["-p", "EI"], 2, "Missing option '--date_debut' / '-d'", None),
        (
            ["-p", "EI", "-d", "2020/02/01", "-f", "2020-09-01"],
            1,
            None,
            "Mauvais format de date détecté",
        ),
    ],
)
def test_cli_parameters(args: list, exit_code: int, output: str, exception: str):
    """test le passage de paramètres à la CLI

    Args:
        args (list): arguments passés au CLI
        exit_code (int): exit_code renvoyé par la CLI
        output (str): message renvoyé par la CLI
        exception (str): exception raise par la CLI
    """
    runner = CliRunner()
    result = runner.invoke(cli=cli, args=args)
    assert result.exit_code == exit_code
    if output:
        assert output in result.output
    if exception:
        assert exception in str(result.exception)


@patch(
    "question_b.BigQueryPartenariatInformationRepository",
    InMemoryPartenariatInformationRepository,
)
@pytest.mark.parametrize(
    "args, exit_code, expected_result",
    [
        (
            ["-p", "EI", "-d", "2020-02-01", "-f", "2020-09-01"],
            0,
            [
                {"partnership_type": "EI", "departement": 72, "number_of_lessons": 3},
                {"partnership_type": "EI", "departement": 73, "number_of_lessons": 4},
                {"partnership_type": "EI", "departement": 74, "number_of_lessons": 5},
                {"partnership_type": "EI", "departement": 75, "number_of_lessons": 6},
            ],
        ),
        (
            ["-p", "EIRL", "-d", "2020-02-01", "-f", "2020-09-01"],
            0,
            [
                {"partnership_type": "EIRL", "departement": 70, "number_of_lessons": 1},
                {"partnership_type": "EIRL", "departement": 71, "number_of_lessons": 2},
            ],
        ),
        (
            ["-p", "EI", "-p", "EIRL", "-d", "2020-02-01", "-f", "2020-09-01"],
            0,
            [
                {"partnership_type": "EI", "departement": 72, "number_of_lessons": 3},
                {"partnership_type": "EI", "departement": 73, "number_of_lessons": 4},
                {"partnership_type": "EI", "departement": 74, "number_of_lessons": 5},
                {"partnership_type": "EI", "departement": 75, "number_of_lessons": 6},
                {"partnership_type": "EIRL", "departement": 70, "number_of_lessons": 1},
                {"partnership_type": "EIRL", "departement": 71, "number_of_lessons": 2},
            ],
        ),
    ],
)
def test_compute_nombre_lecon_par_departement(
    args: list, exit_code: int, expected_result: list
):
    """test le comportement du calcul du nombre de lecon par département

    Args:
        args (list): arguments passés au CLI
        exit_code (int): exit_code renvoyé par la CLI
        expected_result (list): list de dictionnaires contenant le nombre de lecon par partenariat et departement
    """
    runner = CliRunner()
    result = runner.invoke(cli=cli, args=args)
    assert eval(result.output) == expected_result
    assert result.exit_code == exit_code
