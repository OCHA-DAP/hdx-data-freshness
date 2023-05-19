import itertools
import logging
from datetime import timedelta

from dateutil.relativedelta import relativedelta
from dateutil.rrule import MONTHLY, WE, WEEKLY, rrule
from hdx.utilities.dictandlist import write_list_to_csv
from sqlalchemy import delete, select

from ..database.dbdataset import DBDataset
from ..database.dbresource import DBResource
from ..database.dbrun import DBRun

logger = logging.getLogger(__name__)


class DBClean:
    # Keep daily runs up to 2 years back, weekly from 2 years to 4 years and
    # monthly thereafter. Also keep a couple of runs around the end of each
    # quarter.
    def __init__(self, session):
        self.session = session

    def get_runs(self):
        return self.session.scalars(
            select(DBRun).distinct().order_by(DBRun.run_number.desc())
        ).all()

    def get_dataset_runs(self):
        return self.session.scalars(
            select(DBDataset.run_number)
            .distinct()
            .order_by(DBDataset.run_number.desc())
        ).all()

    def get_resource_runs(self):
        return self.session.scalars(
            select(DBResource.run_number)
            .distinct()
            .order_by(DBResource.run_number.desc())
        ).all()

    def run(
        self,
        now,
        check_enddate=True,
        filepath="runs.csv",
        fail_on_run_difference=True,
    ):
        list_run_numbers = self.get_runs()
        end_date = list_run_numbers[0].run_date
        if check_enddate and (now - end_date) > timedelta(days=2):
            logger.error(
                f"End date {end_date.isoformat()} from database is not close to current date!"
            )
            return False
        dataset_run_numbers = self.get_dataset_runs()
        resource_run_numbers = self.get_resource_runs()
        if (
            fail_on_run_difference
            and dataset_run_numbers != resource_run_numbers
        ):
            s = set(resource_run_numbers)
            diff = [x for x in dataset_run_numbers if x not in s]
            logger.error(
                f"Dataset and resource run numbers do not match! Differences: {diff}"
            )
            return False

        two_years_ago = now - relativedelta(years=2)
        four_years_ago = now - relativedelta(years=4)
        runs_to_keep = {0, 1}
        run_date_to_run_number = {}
        run_number_to_run_date = {}
        run_date = None
        overaday = timedelta(days=1, hours=12)
        for run_number in list_run_numbers:
            run_no = run_number.run_number
            if (
                run_no not in dataset_run_numbers
                or run_no not in resource_run_numbers
            ):
                logger.warning(
                    f"Run number {run_number} with date {run_date} is probably broken!"
                )
            previous_run_date = run_date
            run_date = run_number.run_date
            if previous_run_date and (previous_run_date - run_date) > overaday:
                logger.warning(
                    f"Break in runs around run number {run_number} with date {run_date}"
                )
            run_number_to_run_date[run_no] = run_date
            if run_date >= two_years_ago:
                runs_to_keep.add(run_no)
                continue
            run_date_to_run_number[run_date.date()] = run_no

        week_dates = rrule(
            WEEKLY, dtstart=four_years_ago, until=two_years_ago, byweekday=WE
        )
        start_date = list_run_numbers[-1].run_date
        month_dates = list(
            rrule(
                MONTHLY, dtstart=start_date, until=two_years_ago, bymonthday=-1
            )
        )
        end_quarter_dates = [
            d for d in month_dates if d.month in (3, 6, 9, 12)
        ]
        start_quarter_dates = [
            d + timedelta(days=1) for d in end_quarter_dates
        ]

        def get_dayoffsets(n):
            day_offsets = [0]
            for i in range(1, n + 1):
                day_offsets.append(-i)
                day_offsets.append(i)
            return day_offsets

        def keep_runs(dts, day_offsets):
            for dt in dts:
                run_no = None
                for day_offset in day_offsets:
                    run_date = dt + relativedelta(days=day_offset)
                    run_no = run_date_to_run_number.get(run_date.date())
                    if run_no is not None and run_no not in runs_to_keep:
                        if (
                            run_no not in dataset_run_numbers
                            or run_no not in resource_run_numbers
                        ):
                            logger.warning(
                                f"Won't keep broken run number {run_no} with date {run_date}!"
                            )
                            continue
                        break
                if run_no:
                    runs_to_keep.add(run_no)

        keep_runs(week_dates, day_offsets=get_dayoffsets(2))
        keep_runs(month_dates, day_offsets=get_dayoffsets(7))
        keep_runs(start_quarter_dates, day_offsets=get_dayoffsets(14))
        keep_runs(end_quarter_dates, day_offsets=get_dayoffsets(14))

        def to_ranges(iterable):
            iterable = sorted(set(iterable))
            for key, group in itertools.groupby(
                enumerate(iterable), lambda t: t[1] - t[0]
            ):
                group = list(group)
                yield group[0][1], group[-1][1]

        dates_ranges = list()
        for start, end in to_ranges(runs_to_keep):
            start_date = run_number_to_run_date[start].date().isoformat()
            if start == end:
                dates_ranges.append(f"{start_date}, ")
            else:
                end_date = run_number_to_run_date[end].date().isoformat()
                if end_date == start_date:
                    dates_ranges.append(f"{start_date}, ")
                else:
                    dates_ranges.append(f"\n{start_date} to {end_date},\n")
        run_dates_to_keep = "".join(dates_ranges)
        logger.info(f"Keeping these dates:\n{run_dates_to_keep}")

        rows = []
        for run_number, run_date in run_number_to_run_date.items():
            row = [run_number, run_date]
            if run_number in runs_to_keep:
                row.append("N")
            else:
                row.append("Y")
            rows.append(row)
        write_list_to_csv(
            filepath, rows, headers=("Run Number", "Run Date", "Delete")
        )

        for run_number, run_date in run_number_to_run_date.items():
            if run_number not in runs_to_keep:
                self.session.execute(
                    delete(DBResource).where(
                        DBResource.run_number == run_number
                    )
                )
                self.session.execute(
                    delete(DBDataset).where(DBDataset.run_number == run_number)
                )
                self.session.execute(
                    delete(DBRun).where(DBRun.run_number == run_number)
                )
                self.session.commit()
        return True
