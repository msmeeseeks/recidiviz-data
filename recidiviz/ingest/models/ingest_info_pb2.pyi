# @generated by generate_proto_mypy_stubs.py.  Do not edit!
from google.protobuf.internal.containers import (
    RepeatedCompositeFieldContainer as google___protobuf___internal___containers___RepeatedCompositeFieldContainer,
    RepeatedScalarFieldContainer as google___protobuf___internal___containers___RepeatedScalarFieldContainer,
)

from google.protobuf.message import (
    Message as google___protobuf___message___Message,
)

from typing import (
    Iterable as typing___Iterable,
    Optional as typing___Optional,
    Text as typing___Text,
)


class IngestInfo(google___protobuf___message___Message):

    @property
    def people(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Person]: ...

    @property
    def bookings(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Booking]: ...

    @property
    def arrests(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Arrest]: ...

    @property
    def charges(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Charge]: ...

    @property
    def holds(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Hold]: ...

    @property
    def bonds(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Bond]: ...

    @property
    def sentences(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Sentence]: ...

    def __init__(self,
        people : typing___Optional[typing___Iterable[Person]] = None,
        bookings : typing___Optional[typing___Iterable[Booking]] = None,
        arrests : typing___Optional[typing___Iterable[Arrest]] = None,
        charges : typing___Optional[typing___Iterable[Charge]] = None,
        holds : typing___Optional[typing___Iterable[Hold]] = None,
        bonds : typing___Optional[typing___Iterable[Bond]] = None,
        sentences : typing___Optional[typing___Iterable[Sentence]] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> IngestInfo: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Person(google___protobuf___message___Message):
    person_id = ... # type: typing___Text
    surname = ... # type: typing___Text
    given_names = ... # type: typing___Text
    birthdate = ... # type: typing___Text
    gender = ... # type: typing___Text
    age = ... # type: typing___Text
    race = ... # type: typing___Text
    ethnicity = ... # type: typing___Text
    place_of_residence = ... # type: typing___Text
    full_name = ... # type: typing___Text
    middle_names = ... # type: typing___Text
    booking_ids = ... # type: google___protobuf___internal___containers___RepeatedScalarFieldContainer[typing___Text]

    def __init__(self,
        person_id : typing___Optional[typing___Text] = None,
        surname : typing___Optional[typing___Text] = None,
        given_names : typing___Optional[typing___Text] = None,
        birthdate : typing___Optional[typing___Text] = None,
        gender : typing___Optional[typing___Text] = None,
        age : typing___Optional[typing___Text] = None,
        race : typing___Optional[typing___Text] = None,
        ethnicity : typing___Optional[typing___Text] = None,
        place_of_residence : typing___Optional[typing___Text] = None,
        full_name : typing___Optional[typing___Text] = None,
        middle_names : typing___Optional[typing___Text] = None,
        booking_ids : typing___Optional[typing___Iterable[typing___Text]] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Person: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Booking(google___protobuf___message___Message):
    booking_id = ... # type: typing___Text
    admission_date = ... # type: typing___Text
    admission_reason = ... # type: typing___Text
    projected_release_date = ... # type: typing___Text
    release_date = ... # type: typing___Text
    release_reason = ... # type: typing___Text
    custody_status = ... # type: typing___Text
    facility = ... # type: typing___Text
    classification = ... # type: typing___Text
    total_bond_amount = ... # type: typing___Text
    arrest_id = ... # type: typing___Text
    charge_ids = ... # type: google___protobuf___internal___containers___RepeatedScalarFieldContainer[typing___Text]
    hold_ids = ... # type: google___protobuf___internal___containers___RepeatedScalarFieldContainer[typing___Text]

    def __init__(self,
        booking_id : typing___Optional[typing___Text] = None,
        admission_date : typing___Optional[typing___Text] = None,
        admission_reason : typing___Optional[typing___Text] = None,
        projected_release_date : typing___Optional[typing___Text] = None,
        release_date : typing___Optional[typing___Text] = None,
        release_reason : typing___Optional[typing___Text] = None,
        custody_status : typing___Optional[typing___Text] = None,
        facility : typing___Optional[typing___Text] = None,
        classification : typing___Optional[typing___Text] = None,
        total_bond_amount : typing___Optional[typing___Text] = None,
        arrest_id : typing___Optional[typing___Text] = None,
        charge_ids : typing___Optional[typing___Iterable[typing___Text]] = None,
        hold_ids : typing___Optional[typing___Iterable[typing___Text]] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Booking: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Arrest(google___protobuf___message___Message):
    arrest_id = ... # type: typing___Text
    arrest_date = ... # type: typing___Text
    location = ... # type: typing___Text
    officer_name = ... # type: typing___Text
    officer_id = ... # type: typing___Text
    agency = ... # type: typing___Text

    def __init__(self,
        arrest_id : typing___Optional[typing___Text] = None,
        arrest_date : typing___Optional[typing___Text] = None,
        location : typing___Optional[typing___Text] = None,
        officer_name : typing___Optional[typing___Text] = None,
        officer_id : typing___Optional[typing___Text] = None,
        agency : typing___Optional[typing___Text] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Arrest: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Charge(google___protobuf___message___Message):
    charge_id = ... # type: typing___Text
    offense_date = ... # type: typing___Text
    statute = ... # type: typing___Text
    name = ... # type: typing___Text
    attempted = ... # type: typing___Text
    degree = ... # type: typing___Text
    charge_class = ... # type: typing___Text
    level = ... # type: typing___Text
    fee_dollars = ... # type: typing___Text
    charging_entity = ... # type: typing___Text
    status = ... # type: typing___Text
    number_of_counts = ... # type: typing___Text
    court_type = ... # type: typing___Text
    case_number = ... # type: typing___Text
    next_court_date = ... # type: typing___Text
    judge_name = ... # type: typing___Text
    charge_notes = ... # type: typing___Text
    bond_id = ... # type: typing___Text
    sentence_id = ... # type: typing___Text

    def __init__(self,
        charge_id : typing___Optional[typing___Text] = None,
        offense_date : typing___Optional[typing___Text] = None,
        statute : typing___Optional[typing___Text] = None,
        name : typing___Optional[typing___Text] = None,
        attempted : typing___Optional[typing___Text] = None,
        degree : typing___Optional[typing___Text] = None,
        charge_class : typing___Optional[typing___Text] = None,
        level : typing___Optional[typing___Text] = None,
        fee_dollars : typing___Optional[typing___Text] = None,
        charging_entity : typing___Optional[typing___Text] = None,
        status : typing___Optional[typing___Text] = None,
        number_of_counts : typing___Optional[typing___Text] = None,
        court_type : typing___Optional[typing___Text] = None,
        case_number : typing___Optional[typing___Text] = None,
        next_court_date : typing___Optional[typing___Text] = None,
        judge_name : typing___Optional[typing___Text] = None,
        charge_notes : typing___Optional[typing___Text] = None,
        bond_id : typing___Optional[typing___Text] = None,
        sentence_id : typing___Optional[typing___Text] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Charge: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Hold(google___protobuf___message___Message):
    hold_id = ... # type: typing___Text
    jurisdiction_name = ... # type: typing___Text
    status = ... # type: typing___Text

    def __init__(self,
        hold_id : typing___Optional[typing___Text] = None,
        jurisdiction_name : typing___Optional[typing___Text] = None,
        status : typing___Optional[typing___Text] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Hold: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Bond(google___protobuf___message___Message):
    bond_id = ... # type: typing___Text
    amount = ... # type: typing___Text
    bond_type = ... # type: typing___Text
    status = ... # type: typing___Text
    bond_agent = ... # type: typing___Text

    def __init__(self,
        bond_id : typing___Optional[typing___Text] = None,
        amount : typing___Optional[typing___Text] = None,
        bond_type : typing___Optional[typing___Text] = None,
        status : typing___Optional[typing___Text] = None,
        bond_agent : typing___Optional[typing___Text] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Bond: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...

class Sentence(google___protobuf___message___Message):
    sentence_id = ... # type: typing___Text
    min_length = ... # type: typing___Text
    max_length = ... # type: typing___Text
    is_life = ... # type: typing___Text
    is_probation = ... # type: typing___Text
    is_suspended = ... # type: typing___Text
    fine_dollars = ... # type: typing___Text
    parole_possible = ... # type: typing___Text
    post_release_supervision_length = ... # type: typing___Text
    sentencing_region = ... # type: typing___Text
    status = ... # type: typing___Text

    def __init__(self,
        sentence_id : typing___Optional[typing___Text] = None,
        min_length : typing___Optional[typing___Text] = None,
        max_length : typing___Optional[typing___Text] = None,
        is_life : typing___Optional[typing___Text] = None,
        is_probation : typing___Optional[typing___Text] = None,
        is_suspended : typing___Optional[typing___Text] = None,
        fine_dollars : typing___Optional[typing___Text] = None,
        parole_possible : typing___Optional[typing___Text] = None,
        post_release_supervision_length : typing___Optional[typing___Text] = None,
        sentencing_region : typing___Optional[typing___Text] = None,
        status : typing___Optional[typing___Text] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Sentence: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
