import { ChangeDetectorRef, EventEmitter, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
import { Subject } from 'rxjs';
import { NzDropDownComponent } from '../dropdown/nz-dropdown.component';
import { NzI18nInterface } from '../i18n/nz-i18n.interface';
import { NzI18nService } from '../i18n/nz-i18n.service';
export declare type NzThFilterType = Array<{
    text: string;
    value: any;
    byDefault?: boolean;
}>;
export interface NzThItemInterface {
    text: string;
    value: any;
    checked: boolean;
}
export declare class NzThComponent implements OnChanges, OnInit, OnDestroy {
    private cdr;
    private i18n;
    hasFilterValue: boolean;
    filterVisible: boolean;
    multipleFilterList: NzThItemInterface[];
    singleFilterList: NzThItemInterface[];
    locale: NzI18nInterface['Table'];
    nzWidthChange$: Subject<{}>;
    private destroy$;
    private hasDefaultFilter;
    nzDropDownComponent: NzDropDownComponent;
    nzSelections: Array<{
        text: string;
        onSelect(...args: any[]): any;
    }>;
    nzChecked: boolean;
    nzDisabled: boolean;
    nzIndeterminate: boolean;
    nzSortKey: string;
    nzFilterMultiple: boolean;
    nzWidth: string;
    nzLeft: string;
    nzRight: string;
    nzAlign: 'left' | 'right' | 'center';
    nzSort: 'ascend' | 'descend' | null;
    nzFilters: NzThFilterType;
    nzExpand: boolean;
    nzShowCheckbox: boolean;
    nzCustomFilter: boolean;
    nzShowSort: boolean;
    nzShowFilter: boolean;
    nzShowRowSelection: boolean;
    readonly nzCheckedChange: EventEmitter<boolean>;
    readonly nzSortChange: EventEmitter<string | null>;
    readonly nzSortChangeWithKey: EventEmitter<{
        key: string;
        value: string | null;
    }>;
    readonly nzFilterChange: EventEmitter<any>;
    updateSortValue(): void;
    setSortValue(value: 'ascend' | 'descend' | null): void;
    readonly filterList: NzThItemInterface[];
    readonly filterValue: any;
    updateFilterStatus(): void;
    search(): void;
    reset(): void;
    checkMultiple(filter: NzThItemInterface): void;
    checkSingle(filter: NzThItemInterface): void;
    hideDropDown(): void;
    dropDownVisibleChange(value: boolean): void;
    initMultipleFilterList(force?: boolean): void;
    initSingleFilterList(force?: boolean): void;
    checkDefaultFilters(): void;
    marForCheck(): void;
    constructor(cdr: ChangeDetectorRef, i18n: NzI18nService);
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    ngOnDestroy(): void;
}
