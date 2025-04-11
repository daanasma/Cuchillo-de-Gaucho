# Conversions


def kilo_to_giga(kilo: float) -> float:
    return kilo / 1_000_000
def kilo_to_mega(kilo: float) -> float:
    return kilo / 1_000

def giga_to_kilo(giga: float) -> float:
    return giga * 1_000_000

def giga_to_mega(giga: float) -> float:
    return giga * 1_000

def gwh_to_tj(gwh: float) -> float:
    """
    Convert gigawatt-hours (GWh) to terajoules (TJ).

    Parameters:
        gwh (float): Value in gigawatt-hours to be converted to terajoules.

    Returns:
        float: Value converted to terajoules.
    """
    return gwh * 3.6

def gwh_to_kwh(gwh: float) -> float:
    """
    Convert gigawatt-hours (GWh) to kilowatt-hours (KWh).

    Parameters:
        gwh (float): Value in gigawatt-hours to be converted to terajoules.

    Returns:
        float: Value converted to KWh.
    """
    return gwh * 1000000


def tj_to_gwh(tj: float) -> float:
    """
    Convert terajoules (TJ) to gigawatt-hours (GWh).

    Parameters:
        tj (float): Value in terajoules to be converted to gigawatt-hours.

    Returns:
        float: Value converted to gigawatt-hours.
    """
    return tj / 3.6

def gj_to_gwh(gj: float) -> float:
    """
    Convert gigajoules (GJ) to gigawatt-hours (GWh).

    Parameters:
        gj (float): Value in gigajoules to be converted to gigawatt-hours.

    Returns:
        float: Value converted to gigawatt-hours.
    """
    return gj / 3600

def pj_to_gwh(pj: float) -> float:
    """
    Convert petajoules (PJ) to gigawatt-hours (GWh).

    Parameters:
        pj (float): Value in petajoules to be converted to gigawatt-hours.

    Returns:
        float: Value converted to gigawatt-hours.
    """
    return pj * 277.778

def ktoe_to_gwh(ktoe: float, round: bool=False) -> float:
    """
    Convert kiloton of oil equivalent (ktoe) to gigawatt-hours (GWh).

    Parameters:
        ktoe (float): Value in kiloton of oil equivalent to be converted to gigawatt-hours.

    Returns:
        float: Value converted to gigawatt-hours.
    """
    # 1 ktoe is approximately equal to 11.63 GWh
    gwh = ktoe * 11.63
    if round:
        gwh = int(gwh)
    return gwh

def mw_to_gwh(mw: float) -> float:
    return mw * 8760 / 1000

def vollast_percentage_to_yearly_hours(percentage_decimal):
    return percentage_decimal * 8760
