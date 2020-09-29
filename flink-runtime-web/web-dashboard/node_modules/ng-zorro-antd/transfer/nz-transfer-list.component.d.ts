import { ChangeDetectorRef, ElementRef, EventEmitter, OnChanges, OnInit, SimpleChanges, TemplateRef } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { TransferItem } from './interface';
export declare class NzTransferListComponent implements OnChanges, OnInit {
    private el;
    private updateHostClassService;
    private cdr;
    direction: string;
    titleText: string;
    dataSource: TransferItem[];
    itemUnit: string;
    itemsUnit: string;
    filter: string;
    disabled: boolean;
    showSearch: boolean;
    searchPlaceholder: string;
    notFoundContent: string;
    filterOption: (inputValue: string, item: TransferItem) => boolean;
    render: TemplateRef<void>;
    footer: TemplateRef<void>;
    readonly handleSelectAll: EventEmitter<boolean>;
    readonly handleSelect: EventEmitter<TransferItem>;
    readonly filterChange: EventEmitter<{
        direction: string;
        value: string;
    }>;
    prefixCls: string;
    setClassMap(): void;
    stat: {
        checkAll: boolean;
        checkHalf: boolean;
        checkCount: number;
        shownCount: number;
    };
    onHandleSelectAll(status: boolean): void;
    private updateCheckStatus;
    handleFilter(value: string): void;
    handleClear(): void;
    private matchFilter;
    constructor(el: ElementRef, updateHostClassService: NzUpdateHostClassService, cdr: ChangeDetectorRef);
    ngOnChanges(changes: SimpleChanges): void;
    ngOnInit(): void;
    markForCheck(): void;
    _handleSelect(item: TransferItem): void;
}
