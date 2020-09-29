import { ElementRef, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
export declare class NzTdComponent implements OnChanges {
    private elementRef;
    private nzUpdateHostClassService;
    nzChecked: boolean;
    nzDisabled: boolean;
    nzIndeterminate: boolean;
    nzLeft: string;
    nzRight: string;
    nzAlign: 'left' | 'right' | 'center';
    nzIndentSize: number;
    nzExpand: boolean;
    nzShowExpand: boolean;
    nzShowCheckbox: boolean;
    readonly nzCheckedChange: EventEmitter<boolean>;
    readonly nzExpandChange: EventEmitter<boolean>;
    expandChange(e: Event): void;
    setClassMap(): void;
    constructor(elementRef: ElementRef, nzUpdateHostClassService: NzUpdateHostClassService);
    ngOnChanges(changes: SimpleChanges): void;
}
