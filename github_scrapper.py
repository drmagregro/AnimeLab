import requests
import time

gh_token_file = open("token/gh-token.txt")
GITHUB_TOKEN = gh_token_file.readline()
OWNER = "drmagregro"       # ou votre username
REPO = "AnimeLab"
WORKFLOW_NAME = "workflows/main.yml"         # nom du fichier workflow, ou son ID

headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

def get_latest_run(workflow_file: str, branch: str = "main") -> dict | None:
    """Récupère le dernier run d'un workflow donné."""
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows/{workflow_file}/runs"
    params = {"branch": branch, "per_page": 1}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    runs = response.json().get("workflow_runs", [])
    return runs[0] if runs else None

def check_run_status(run: dict) -> str:
    """
    Retourne l'état lisible du run.
    - status     : 'queued' | 'in_progress' | 'completed'
    - conclusion : 'success' | 'failure' | 'cancelled' | 'skipped' | None
    """
    status = run["status"]
    conclusion = run.get("conclusion")  # None si pas encore terminé

    if status != "completed":
        return f"En cours ({status})"
    return f"Terminé : {conclusion}"

# Test final
# --- Utilisation ---
run = get_latest_run(WORKFLOW_NAME, branch="main")
if run:
    print(f"Run #{run['run_number']} — {run['name']}")
    print(f"Branche    : {run['head_branch']}")
    print(f"Déclenché  : {run['created_at']}")
    print(f"Statut     : {check_run_status(run)}")
    print(f"URL        : {run['html_url']}")