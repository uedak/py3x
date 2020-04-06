from . import *  # noqa
from . import Error, Errors

BAD_CHARS = Error('使用できない文字 %s が含まれています')
BAD_FORMAT = Error('形式にエラーがあります')
BLANK = Error('入力されていません')
CONFLICT = Error('別の更新があったため保存に失敗しました')
INVALID = Error('不正な値です')
TAKEN = Error('すでに使用されています')
TOO_EARLY = Error('%s またはそれ以降を選択してください')
TOO_GREAT = Error('%s 以下の値を入力してください')
TOO_LATE = Error('%s またはそれ以前を選択してください')
TOO_LITTLE = Error('%s 以上の値を入力してください')
TOO_LONG = Error('%s 文字以内で入力してください (現在 %s 文字)')
TOO_SHORT = Error('%s 文字以上で入力してください (現在 %s 文字)')
UNSELECTED = Error('選択されていません')


class Errors(Errors):
    __slots__ = ()
    _WS = {
        "\t": '[タブ文字]',
        "\n": '[改行]',
        "\r": '[改行]',
        ' ': '[半角SP]',
        '　': '[全角SP]',
    }
