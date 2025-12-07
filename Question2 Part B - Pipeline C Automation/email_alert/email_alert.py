from email.message import EmailMessage
import ssl
import smtplib
import os
from airflow.sdk import Variable


## function to send email
def _send_email(subject: str, body: str, to: str | list):
    try:
        email_sender = Variable.get("email_sender", default=None)
        email_password = Variable.get("email_password", default=None)
        smtp_host = Variable.get("smtp_host", default=None)
        smtp_port = Variable.get("smtp_port", default=None)
    except Exception as e:
        print(f"Error retrieving from Variable API: {e}")
        email_sender = None
        email_password = None
        smtp_host = None
        smtp_port = None

    # Fallback to environment variables in .env
    email_sender = email_sender or os.environ.get("EMAIL_SENDER")
    email_password = email_password or os.environ.get("EMAIL_PASSWORD")
    smtp_host = smtp_host or os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(smtp_port or os.environ.get("SMTP_PORT", "465"))

    if not email_sender or not email_password:
        print("Email credentials not configured. Skipping send.")
        return

    if isinstance(to, str):
        to = [to]

    em = EmailMessage()
    em["From"] = email_sender
    em["To"] = ", ".join(to)
    em["Subject"] = subject
    em.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as smtp:
            smtp.login(email_sender, email_password)
            smtp.send_message(em)
        print("Email Sent Successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")


## function to send email on task failure alert
def task_fail_alert(context):
    """Send an email when a task fails."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else context.get("dag_run").dag_id
    task_id = ti.task_id if ti else context.get("task").task_id
    state = ti.state if ti else "failed"
    exec_date = context.get("execution_date") or (ti.start_date if ti else None)
    log_url = context.get("task_instance").log_url if context.get("task_instance") else None
    dag_owner = context.get("params", {}).get("dag_owner", "")

    # Try Variable, fallback to env
    try:
        to = Variable.get("email_receiver", default=None)
    except Exception as e:
        print(f"Error retrieving email_receiver from Variable API: {e}")
        to = None

    to = to or os.environ.get("EMAIL_RECEIVER")

    if not to:
        print("No email_receiver configured. Skipping failure alert.")
        return

    subject = f"[Airflow] Task failed: {task_id} in DAG {dag_id}"
    body = f"""Hello {dag_owner}

Task {task_id} in DAG {dag_id} has failed.

State: {state}
Execution date: {exec_date}
Log: {log_url}

Regards,
interswitch
"""
    _send_email(subject, body, to)


## function to send email on dag success alert
def dag_success_alert(context):
    """
    Send a single email when the DAG run succeeds (all tasks succeeded).
    """
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id if dag_run else context.get("task_instance").dag_id
    run_id = dag_run.run_id if dag_run else context.get("run_id")
    start_date = dag_run.start_date if dag_run else None
    dag_owner = context.get("params", {}).get("dag_owner", "")


    try:
        to = Variable.get("email_receiver", default=None)
    except Exception as e:
        print(f"Error retrieving email_receiver from Variable API: {e}")
        to = None

    to = to or os.environ.get("EMAIL_RECEIVER")

    if not to:
        print("No email_receiver configured. Skipping success alert.")
        return

    load_status = None
    try:
        if context.get("ti"):
            load_status = context["ti"].xcom_pull(task_ids="load_clickhouse", key="load_status_message")
        elif dag_run:
            ti = dag_run.get_task_instance("load_clickhouse")
            if ti:
                load_status = ti.xcom_pull(task_ids="load_clickhouse", key="load_status_message")
    except Exception:
        load_status = None

    subject = f"[Airflow] DAG succeeded: {dag_id} ({run_id})"
    body = f"""Hello {dag_owner}

DAG {dag_id} succeeded.

Run id: {run_id}
Start: {start_date}
Load status: {load_status}

Regards,
interswitch
"""
    _send_email(subject, body, to)