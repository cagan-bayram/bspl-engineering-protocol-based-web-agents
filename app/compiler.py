"""
BSPL Compiler
==============

This module provides a simple entry point for compiling BSPL
(``*.bspl``) protocol specifications into artifacts consumable by the
rest of this project.  The goal of the compiler is to bridge
high‑level protocol definitions with concrete implementations for
agents and front‑end components.

The current implementation leverages the upstream :mod:`bspl` library
to parse and validate BSPL files.  Parsing and verification are
responsibilities of the library; this compiler focuses on inspecting
the resulting specification and generating artefacts.  If you wish to
extend the compiler with code generation (e.g. Python classes, JSON
schemas or ASL templates), add your logic in the
``generate_role_specs`` function.

Usage
-----

This module can be executed as a script.  For example, to compile
protocols found in ``protocols/reference`` into an output directory:

.. code:: bash

    python -m app.compiler --input protocols/reference --output build

The script will locate all files with the ``.bspl`` extension under
the input path and attempt to parse them.  Any compiled output will
be written to the output directory (created if it does not exist).

Note
----

The :mod:`bspl` library is not vendored with this project and must be
installed separately.  See ``requirements.txt`` for the editable
installation specification.  At runtime, :func:`compile_protocol`
imports :mod:`bspl` dynamically, so that the module remains importable
even if the dependency is absent (useful for type checking and
auto‑completion).
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable, Optional, Union, List


def find_bspl_files(path: Union[str, Path]) -> Iterable[Path]:
    """Recursively yield all BSPL files beneath *path*.

    Parameters
    ----------
    path:
        A directory or file path.  If a directory is provided, all
        ``*.bspl`` files within it (recursively) will be yielded.  If
        a file is provided and it has a ``.bspl`` suffix, it will be
        yielded directly.  Non‑BSPL files are ignored.

    Yields
    ------
    pathlib.Path
        Paths pointing to BSPL files.
    """
    p = Path(path)
    if p.is_dir():
        for child in p.rglob("*.bspl"):
            yield child
    else:
        if p.suffix == ".bspl":
            yield p


def compile_protocol(bspl_file: Path, output_dir: Path) -> None:
    """Compile a single BSPL file into artefacts.

    This function loads the BSPL specification using the upstream
    library and iterates over the protocols defined within.  For each
    protocol, a role‑specific specification can be generated.  At the
    moment, this implementation simply dumps the parsed protocol into
    a JSON file as a starting point for further work.

    Parameters
    ----------
    bspl_file:
        Path to a BSPL file on disk.
    output_dir:
        Directory into which generated artefacts should be written.
    """
    try:
        import bspl  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "The bspl library is required to compile protocols. "
            "Install it using pip or add it to your environment."
        ) from exc

    # Load specification from file.  According to the upstream API,
    # bspl.load_file returns a Specification object containing one or
    # more Protocol instances.  The specification is validated
    # automatically during loading.
    spec = bspl.load_file(str(bspl_file))

    # Determine a collection of protocol names.  The Specification
    # interface exposes an ``export`` method that can be used to
    # generate Python modules for each protocol.  We attempt to
    # discover all protocol names by checking common attributes.  If
    # none are found, we fall back to exporting a single protocol
    # assumed to have the same stem as the filename.
    protocol_names: Iterable[str] = []
    # Attempt to derive protocol names from the Specification object.
    # Newer versions of BSPL expose ``protocol_names``; otherwise we
    # inspect the ``protocols`` dict.  If neither is available, use
    # the filename stem as a fallback.
    if hasattr(spec, "protocol_names"):
        # ``protocol_names`` is expected to be an iterable of names
        protocol_names = list(getattr(spec, "protocol_names"))  # type: ignore[call-arg]
    elif hasattr(spec, "protocols"):
        protos = getattr(spec, "protocols")  # type: ignore[call-arg]
        # If protocols is a dict, use its keys; if it's an iterable of
        # Protocol instances, extract their names.
        if isinstance(protos, dict):
            protocol_names = list(protos.keys())
        else:
            protocol_names = [p.name for p in protos]
    else:
        protocol_names = [bspl_file.stem]

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    for name in protocol_names:
        # Export protocol.  The export function returns a Protocol
        # instance (rather than a Python module), which encapsulates
        # roles, messages and parameters.  We'll write a top‑level
        # summary and then delegate to :func:`generate_role_specs` for
        # role‑specific details.
        protocol_obj = spec.export(name)

        # Build a high‑level summary of protocol components for quick reference
        # Extract role and message names directly from the protocol object.
        roles = list(protocol_obj.roles.keys())
        messages = list(protocol_obj.messages.keys())

        protocol_data = {
            "protocol": name,
            "source": str(bspl_file),
            "roles": roles,
            "messages": messages,
            "keys": list(protocol_obj.keys),
        }

        # Write the protocol summary as JSON
        json_path = output_dir / f"{name}.json"
        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(protocol_data, fh, indent=2)

        # Print the path to the summary without attempting to relativise it
        print(f"Compiled protocol '{name}' -> {json_path}")

        # Generate role‑specific specifications into a subdirectory
        role_dir = output_dir / name
        role_dir.mkdir(parents=True, exist_ok=True)
        generate_role_specs(protocol_obj, role_dir)


def generate_role_specs(protocol_obj: object, output_dir: Path) -> None:
    """Generate role‑specific specifications for a protocol.

    For each role defined in the protocol, this function derives the
    set of messages that the role can send or receive, along with
    parameter metadata (adornment and key status).  The resulting
    specifications are written as JSON files into *output_dir*.

    The structure of the generated JSON for each role is as follows as an example:

        {
          "role": "Merchant",
          "protocol": "Logistics",
          "keys": ["orderID", "itemID"],
          "messages": [
            {
              "name": "RequestLabel",
              "direction": "send",
              "parameters": [
                {"name": "orderID", "adornment": "out", "key": true},
                {"name": "address", "adornment": "out", "key": false}
              ],
              "ins": [],
              "outs": ["orderID", "address"],
              "nils": []
            },
            ...
          ]
        }

    Parameters
    ----------
    protocol_obj:
        A :class:`bspl.protocol.Protocol` instance returned by
        :meth:`bspl.Specification.export`.
    output_dir:
        Directory where role specifications should be written.  A file
        named ``<role>.json`` will be created for each role.
    """
    # Each protocol defines a mapping of role names to Role instances.
    for role_name, role in getattr(protocol_obj, "roles", {}).items():
        # Determine protocol keys at the protocol level.  The BSPL
        # library exposes both a ``get_keys`` method and a ``keys``
        # property.  Each returns a dictionary mapping key names to
        # Parameter objects.  Convert to a list of names.
        protocol_keys: List[str]
        try:
            keys_source = None
            # Prefer get_keys() if it exists
            if callable(getattr(protocol_obj, "get_keys", None)):
                keys_source = protocol_obj.get_keys()  # type: ignore[attr-defined]
            else:
                keys_source = getattr(protocol_obj, "keys", {})  # type: ignore[attr-defined]
            if isinstance(keys_source, dict):
                protocol_keys = list(keys_source.keys())
            else:
                # If keys_source is iterable (e.g. a set), convert directly
                protocol_keys = list(keys_source)
        except Exception:
            # Fallback: derive keys from parameters if neither method is available
            protocol_keys = [p.name for p in getattr(protocol_obj, "parameters", {}).values() if getattr(p, "key", False)]

        messages_data = []
        # The Role.messages method returns a mapping of message names to
        # Message objects relevant to this role (both sends and receives).
        msg_mapping = {}
        if hasattr(role, "messages"):
            try:
                msg_mapping = role.messages(protocol_obj)  # type: ignore[call-arg]
            except Exception:
                msg_mapping = {}

        for msg_name, msg in msg_mapping.items():
            # Determine the direction relative to this role.  A role
            # sends a message if it is the sender; otherwise, if it
            # appears in recipients it receives the message.
            direction = "send" if getattr(msg, "sender", None) and getattr(msg.sender, "name", None) == role_name else "receive"

            # Gather parameter metadata.  We iterate over the message's
            # public parameters to avoid exposing private variables.
            params_data = []
            public_parameters = getattr(msg, "public_parameters", {})
            for param_name, param in public_parameters.items():
                params_data.append({
                    "name": param_name,
                    "adornment": getattr(param, "adornment", None),
                    "key": bool(getattr(param, "key", False)),
                })

            # Determine adornment groups.  Messages inherit ``ins``,
            # ``outs`` and ``nils`` from the Protocol base; these are
            # sets of parameter names.
            ins_params = list(getattr(msg, "ins", []))
            outs_params = list(getattr(msg, "outs", []))
            nil_params = list(getattr(msg, "nils", []))

            messages_data.append({
                "name": msg_name,
                "direction": direction,
                "parameters": params_data,
                "ins": ins_params,
                "outs": outs_params,
                "nils": nil_params,
            })

        role_spec = {
            "role": role_name,
            "protocol": getattr(protocol_obj, "name", None),
            "keys": protocol_keys,
            "messages": messages_data,
        }

        # Write the role specification to disk as JSON.  Use role name
        # as filename to avoid collisions.
        out_file = output_dir / f"{role_name}.json"
        with out_file.open("w", encoding="utf-8") as fh:
            json.dump(role_spec, fh, indent=2)

        # Report the output path without calling relative_to, which may raise
        # errors if the output directory is not under the current working directory
        print(f"  Wrote role spec for {role_name} -> {out_file}")


def main(argv: Optional[Iterable[str]] = None) -> None:
    """Entry point for the compiler when run as a script.

    Accepts command‑line arguments specifying the location of BSPL
    protocols and the output directory.  Processes all files found
    under the input path.
    """
    parser = argparse.ArgumentParser(description="Compile BSPL protocols")
    parser.add_argument(
        "--input",
        "-i",
        default="protocols",
        type=str,
        help="Directory or file containing BSPL specifications",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="build",
        type=str,
        help="Directory to write compiled output",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        parser.error(f"Input path {input_path} does not exist")

    for bspl_file in find_bspl_files(input_path):
        compile_protocol(bspl_file, output_path)


if __name__ == "__main__":
    main()