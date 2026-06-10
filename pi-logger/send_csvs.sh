#!/bin/bash
# Incremental sync of Enviro+ CSVs to a receiving PC over SSH/SCP.
# Asks the PC which CSVs it already has (Windows `dir /b`), then sends only the
# ones it is missing -- in a single scp connection.
#
# Requirements on the receiving PC:
#   - an OpenSSH *server* running and reachable
#   - this Pi's public key ($KEY.pub) authorized
#   - the target folder existing (the script also tries to create it)
#
# Set the destination in ~/.enviro_pc_dest as one line "user@host:targetdir"
# (targetdir is relative to the PC user's home); otherwise DEST_DEFAULT is used.
set -u

DATA_DIR="$HOME/data"
KEY="$HOME/.ssh/pc_key"
DEST_DEFAULT="user@PC_HOST:enviro_incoming"
CFG="$HOME/.enviro_pc_dest"
SSH_OPTS="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=8"

DEST="$DEST_DEFAULT"
[ -f "$CFG" ] && DEST="$(head -n1 "$CFG" | tr -d '\r\n')"
USERHOST="${DEST%%:*}"
TARGET="${DEST#*:}"
[ "$TARGET" = "$DEST" ] && TARGET="enviro_incoming"

if [ ! -f "$KEY" ]; then
    echo "no PC key ($KEY) - generate one and authorize it on the PC first"
    exit 3
fi

# Make sure the target folder exists on the PC (cmd shell), best effort.
ssh $SSH_OPTS "$USERHOST" "if not exist \"$TARGET\" md \"$TARGET\"" >/dev/null 2>&1

# What the PC already has (basenames, one per line; strip CR from Windows output).
REMOTE="$(ssh $SSH_OPTS "$USERHOST" "dir /b \"$TARGET\"" 2>/dev/null | tr -d '\r')"

declare -A HAVE
while IFS= read -r line; do
    [ -n "$line" ] && HAVE["$line"]=1
done <<< "$REMOTE"

# Build the list of local CSVs the PC does not have yet.
MISSING=()
total=0
for f in "$DATA_DIR"/enviro_log_*.csv; do
    [ -e "$f" ] || continue
    total=$((total + 1))
    b="$(basename "$f")"
    [ -n "${HAVE[$b]:-}" ] && continue
    MISSING+=("$f")
done

n=${#MISSING[@]}
if [ "$n" -eq 0 ]; then
    echo "up to date - all $total CSVs already on PC"
    exit 0
fi

# Send every missing file in a single scp connection.
if scp $SSH_OPTS "${MISSING[@]}" "$USERHOST:$TARGET/" >/dev/null 2>&1; then
    echo "sent $n new (of $total total)"
    exit 0
else
    echo "send failed for some of $n new files - check PC SSH server"
    exit 1
fi
