import { ChangeDetectorRef, EventEmitter, OnChanges } from '@angular/core';
export declare class NzTransferSearchComponent implements OnChanges {
    private cdr;
    placeholder: string;
    value: string;
    disabled: boolean;
    readonly valueChanged: EventEmitter<string>;
    readonly valueClear: EventEmitter<void>;
    constructor(cdr: ChangeDetectorRef);
    _handle(): void;
    _clear(): void;
    ngOnChanges(): void;
}
