from click.testing import CliRunner
from question_b import get_nombre_lecon_par_departement_selon_partenariat

def test_cli_no_arguments():
  runner = CliRunner()
  result = runner.invoke(cli=get_nombre_lecon_par_departement_selon_partenariat, args=[])
  assert result.exit_code != 0
  assert "Missing option '--partnership_type' / '-p'" in result.output

def test_cli_missing_arguments():
  runner = CliRunner()
  result = runner.invoke(cli=get_nombre_lecon_par_departement_selon_partenariat, args=["-p" ,"EI"])
  assert result.exit_code != 0
  assert "Missing option '--date_debut' / '-d'" in result.output

def test_cli_good_arguments():
  runner = CliRunner()
  result = runner.invoke(cli=get_nombre_lecon_par_departement_selon_partenariat, args=["-p", "EI", "-d" ,"2020-02-01","-f" ,"2020-09-01"])
  assert result.exit_code == 0

def test_cli_multiple_partenariat():
  runner = CliRunner()
  result = runner.invoke(cli=get_nombre_lecon_par_departement_selon_partenariat, args=["-p", "EI","-p", "EIRL", "-d" ,"2020-02-01","-f" ,"2020-09-01"])
  assert result.exit_code == 0

def test_cli_wrong_date_format():
  runner = CliRunner()
  result = runner.invoke(cli=get_nombre_lecon_par_departement_selon_partenariat, args=["-p", "EI", "-d" ,"2020/02/01","-f" ,"2020-09-01"])
  assert result.exit_code != 0
  assert "Mauvais format de date détecté" in str(result.exception)