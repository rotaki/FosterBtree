#!/usr/bin/env python3
"""Build a multipart/mixed MIME email with PNG attachments and pipe to sendmail.

Usage:
    _send_email.py TO SUBJECT BODY_FILE [attachment...]
"""
import mimetypes
import os
import subprocess
import sys
from email.message import EmailMessage


def main():
    if len(sys.argv) < 4:
        print("usage: _send_email.py TO SUBJECT BODY_FILE [attachments...]",
              file=sys.stderr)
        sys.exit(2)
    to, subject, body_file = sys.argv[1], sys.argv[2], sys.argv[3]
    attachments = sys.argv[4:]

    msg = EmailMessage()
    msg["From"] = f"rotaki@{os.uname().nodename}"
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(open(body_file).read())

    for path in attachments:
        if not os.path.exists(path):
            print(f"WARN: skipping missing attachment {path}", file=sys.stderr)
            continue
        ctype, _ = mimetypes.guess_type(path)
        maintype, subtype = (ctype or "application/octet-stream").split("/", 1)
        with open(path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=os.path.basename(path),
            )

    raw = msg.as_bytes()
    p = subprocess.Popen(
        ["/usr/sbin/sendmail", "-t", "-oi"],
        stdin=subprocess.PIPE,
    )
    p.communicate(raw)
    sys.exit(p.returncode)


if __name__ == "__main__":
    main()
