# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

"""Constants related to a bond entity."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class BondType(EntityEnum, metaclass=EntityEnumMeta):
    CASH = enum_strings.bond_type_cash
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    NO_BOND = enum_strings.bond_type_no_bond
    PARTIAL_CASH = enum_strings.bond_type_partial_cash
    SECURED = enum_strings.bond_type_secured
    UNKNOWN_REMOVED_FROM_SOURCE = enum_strings.unknown_removed_from_source
    UNSECURED = enum_strings.bond_type_unsecured

    @staticmethod
    def _get_default_map():
        return BOND_TYPE_MAP


class BondStatus(EntityEnum, metaclass=EntityEnumMeta):
    DENIED = enum_strings.bond_status_denied
    INFERRED_SET = enum_strings.bond_status_inferred_set
    NOT_REQUIRED = enum_strings.bond_status_not_required
    PENDING = enum_strings.bond_status_pending
    POSTED = enum_strings.bond_status_posted
    REVOKED = enum_strings.bond_status_revoked
    SET = enum_strings.bond_status_set
    UNKNOWN_FOUND_IN_SOURCE = enum_strings.unknown_found_in_source
    UNKNOWN_REMOVED_FROM_SOURCE = enum_strings.unknown_removed_from_source

    @staticmethod
    def _get_default_map():
        return BOND_STATUS_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# Not marked as private so it can be used in
# persistence/converter/converter_utils
BOND_TYPE_MAP = {
    '10 BOND': BondType.PARTIAL_CASH,
    'BAIL DENIED': BondType.NO_BOND,
    'BOND DENIED': BondType.NO_BOND,
    'BONDING COMPANY': BondType.SECURED,
    'CASH': BondType.CASH,
    'CASH BOND': BondType.CASH,
    'CASH OR SURETY': BondType.SECURED,
    'CASH SURETY': BondType.SECURED,
    'FULL CASH': BondType.CASH,
    'HOLD WITHOUT BAIL': BondType.NO_BOND,
    'N A': BondType.NO_BOND,
    'NO BAIL': BondType.NO_BOND,
    'NO BOND': BondType.NO_BOND,
    'NO BOND ALLOWED': BondType.NO_BOND,
    'NONE SET': BondType.NO_BOND,
    'NONE': BondType.NO_BOND,
    'OTHER': BondType.NO_BOND,
    'OWN RECOG': BondType.NO_BOND,
    'OWN RECOGNIZANCE': BondType.NO_BOND,
    'OWN RECOGNIZANCE SIGNATURE BOND': BondType.NO_BOND,
    'PARTIAL CASH': BondType.PARTIAL_CASH,
    'PAY AND RELEASE': BondType.CASH,
    'PURGE': BondType.CASH,
    'PURGE PAYMENT': BondType.CASH,
    'RELEASE ON RECOGNIZANCE': BondType.NO_BOND,
    'RELEASED BY COURT': BondType.NO_BOND,
    'RELEASED ON OWN RECOGNIZANCE': BondType.NO_BOND,
    'ROR': BondType.NO_BOND,
    'SECURED': BondType.SECURED,
    'SECURE BOND': BondType.SECURED,
    'SECURED BOND': BondType.SECURED,
    'SURETY': BondType.SECURED,
    'SURETY BOND': BondType.SECURED,
    'U S CURRENCY': BondType.CASH,
    'UNKNOWN': BondType.EXTERNAL_UNKNOWN,
    'UNSECURE BOND': BondType.UNSECURED,
    'UNSECURED': BondType.UNSECURED,
    'WRITTEN PROMISE': BondType.NO_BOND,
    'WRITTEN PROMISE TO APPEAR': BondType.NO_BOND,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# Not marked as private so it can be used in
# persistence/converter/converter_utils
BOND_STATUS_MAP = {
    'ACTIVE': BondStatus.SET,
    'BOND DENIED': BondStatus.DENIED,
    'BOND REVOCATION': BondStatus.REVOKED,
    'DENIED': BondStatus.DENIED,
    'HOLD WITHOUT BAIL': BondStatus.DENIED,
    'NO BOND ALLOWED': BondStatus.DENIED,
    'NONE SET': BondStatus.NOT_REQUIRED,
    'NOT REQUIRED': BondStatus.NOT_REQUIRED,
    'PENDING': BondStatus.PENDING,
    'POSTED': BondStatus.POSTED,
    'REVOKED': BondStatus.REVOKED,
    'BOND REVOKED': BondStatus.REVOKED,
    'SET': BondStatus.SET,
}
