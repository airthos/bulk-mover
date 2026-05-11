from dotenv import load_dotenv

import auth
import graph
import prompts
import sync

load_dotenv()


def main() -> None:
    print("\n=== Bulk Mover ===\n")

    token = auth.get_access_token()
    graph.register_token_refresher(auth.get_access_token)

    existing = sync.find_incomplete_runs()
    if existing:
        run_dir, run_config = prompts.prompt_resume_run(existing)
        if run_dir and run_config:
            sync.run(run_config, token, run_dir=run_dir)
            return

    source_config = prompts.prompt_source(token)
    if not source_config.get("source_drive_id"):
        raise RuntimeError("Could not resolve source drive from the pasted URL.")

    dest_config = prompts.prompt_destination(token)
    run_config = {**source_config, **dest_config}

    if not prompts.confirm_run(run_config):
        print("Aborted.")
        return

    sync.run(run_config, token)


if __name__ == "__main__":
    main()
