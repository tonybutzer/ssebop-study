# ===============================================================================
# AUTHOR GABRIEL PARRISH
# ===============================================================================
from datetime import datetime as dt
from datetime import timedelta
from dateutil import relativedelta
from calendar import isleap


"""
 _ _  _  _  ___  ___   ___  ___  ___  ___
| | || || || . \/ __> / __>/ __>| __>| . > ___  ___
| ' || || ||   /\__ \ \__ \\__ \| _> | . \/ . \| . \
|__/ |_||_||_\_\<___/ <___/<___/|___>|___/\___/|  _/
                                               |_|

                                               """

def dekad_lookup(return_breaks=False, custom_year=None):
    """creates a dekad lookup table for leap or non leap"""

    # isleap() function, imported above, helps determine which it is... leap or nonleap...

    # does it look up the dekad by the DOY? - if i make this into an object it could be helpful.
    dekad_lookup = {}


    if not custom_year is None:
        year = int(custom_year)
        if isleap(year):
            days_iy = 366
        else:
            days_iy = 365

    # elif leap:
    #     year = 2000
    #     days_iy = 366
    # else:
    #     year = 2001
    #     days_iy = 365

    dt_start = dt(year=year, month=1, day=1)
    delta_time = timedelta(days=1)
    ts = [dt_start + (i*delta_time) for i in range(days_iy)]
    # print(ts)
    bp_dict = {}
    for t in ts:
        yr = t.year
        mo = t.month
        # construct the breakpoints
        bp1 = dt(yr, mo, 10)
        bp2 = dt(yr, mo, 20)
        # the last breakpoint is the last day of the month.
        # so it's one month from the first day of the month, and then subtract one day gets you the last day of the
        # current month.
        bp3 = dt(yr, mo, 1) + relativedelta.relativedelta(months=+1, days=-1)
        # print('date:', t, 'end of mo breakpoint', bp3)

        # doy will be the dictionary key
        doy = dt.timetuple(t).tm_yday

        if t <= bp1:
            mo_dek = 1
            # 3 dekads in every month preceding the current one, add one for the current dekad
            yr_dek = (3*(mo-1) + 1)
        elif t>bp1 and t <= bp2:
            mo_dek = 2
            yr_dek = (3*(mo-1) + 2)
        elif t > bp2 and t<= bp3:
            mo_dek = 3
            yr_dek = (3*(mo-1) + 3)

        if t == bp1:
            bp_dict[f'{t.month:02}{mo_dek}'] = f'{t.year}-{t.month:02}-{t.day:02}'
        elif t == bp2:
            bp_dict[f'{t.month:02}{mo_dek}'] = f'{t.year}-{t.month:02}-{t.day:02}'
        elif t == bp3:
            bp_dict[f'{t.month:02}{mo_dek}'] = f'{t.year}-{t.month:02}-{t.day:02}'

        # fill out the dictionary with the good info
        dekad_lookup[f'{doy:03}'] = (f'{yr_dek:02}', f'{mo:02}{mo_dek}', f'{t.year}-{t.month}-{t.day}')
    if return_breaks:
        return bp_dict
    else:
        return dekad_lookup


def dekad_to_dt(year=2000, dekad='101', dekad_36=None):
    """"""

    dk_lookup = dekad_lookup(custom_year=year)

    for k, v in dk_lookup.items():

        # print(k)
        # print(v)

        if dekad_36 is None:
            # reverse via the 3 digit dekad not 0-36
            # the last digit of the dekad
            dek_to_match = v[1]
            if dek_to_match == dekad:
                date_str = v[2]
        else:
            dek_to_match = v[0]
            if dek_to_match == dekad_36:
                date_str = v[2]

        # turn the date str into a datetime
    out_dt = dt.strptime(date_str, '%Y-%m-%d')

    return out_dt



# if __name__ == "__main__":
#
#     # dekad_to_dt(year=)

