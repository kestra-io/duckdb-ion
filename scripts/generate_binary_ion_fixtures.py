from pathlib import Path


def ion_ivm() -> bytes:
    return bytes([0xE0, 0x01, 0x00, 0xEA])


def ion_int(value: int) -> bytes:
    if value < 0 or value > 255:
        raise ValueError("value out of supported range for fixture")
    return bytes([0x21, value])


def ion_string(text: str) -> bytes:
    data = text.encode("utf-8")
    if len(data) > 13:
        raise ValueError("string too long for fixture")
    return bytes([0x80 | len(data)]) + data


def ion_var_uint(value: int) -> bytes:
    if value < 0 or value > 127:
        raise ValueError("value out of supported varuint range for fixture")
    return bytes([0x80 | value])


def ion_struct_name(name: str) -> bytes:
    # Uses system symbol ID 4 ("name") for the field name.
    field_sid = ion_var_uint(4)
    value = ion_string(name)
    body = field_sid + value
    if len(body) > 13:
        raise ValueError("struct body too large for fixture")
    return bytes([0xD0 | len(body)]) + body


def ion_list(values: list[bytes]) -> bytes:
    body = b"".join(values)
    # Length 15 is not representable in the nibble (0xF is null), so use 0xE + varuint.
    if len(body) < 14:
        return bytes([0xB0 | len(body)]) + body
    if len(body) > 255:
        raise ValueError("list body too large for fixture")
    return bytes([0xBE]) + ion_var_uint(len(body)) + body


def write_binary(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def main() -> None:
    base = Path("test/ion")
    scalars = ion_ivm() + ion_int(1) + ion_int(2) + ion_int(3)
    write_binary(base / "scalars_binary.ion", scalars)

    struct_alpha = ion_struct_name("alpha")
    struct_beta = ion_struct_name("beta")
    structs = ion_ivm() + struct_alpha + struct_beta
    write_binary(base / "structs_binary.ion", structs)

    array = ion_ivm() + ion_list([struct_alpha, struct_beta])
    write_binary(base / "array_binary.ion", array)


if __name__ == "__main__":
    main()
