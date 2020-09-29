import { EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { NzCalendarI18nInterface } from '../../../i18n/nz-i18n.interface';
import { CandyDate } from '../candy-date';
export declare class DecadePanelComponent implements OnChanges {
    locale: NzCalendarI18nInterface;
    value: CandyDate;
    readonly valueChange: EventEmitter<CandyDate>;
    readonly startYear: number;
    readonly endYear: number;
    prefixCls: string;
    panelDecades: PanelDecadeData[][];
    constructor();
    ngOnChanges(changes: SimpleChanges): void;
    previousCentury(): void;
    nextCentury(): void;
    trackPanelDecade(_index: number, decadeData: PanelDecadeData): string;
    private render;
    private gotoYear;
    private chooseDecade;
    private makePanelDecades;
}
export interface PanelDecadeData {
    content: string;
    title: string;
    isCurrent: boolean;
    isLowerThanStart: boolean;
    isBiggerThanEnd: boolean;
    classMap?: object | null;
    onClick: VoidFunction | null;
}
