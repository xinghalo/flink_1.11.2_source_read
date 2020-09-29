import { ChangeDetectorRef, NgZone, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
import { NzStatisticComponent } from './nz-statistic.component';
export declare class NzCountdownComponent extends NzStatisticComponent implements OnInit, OnChanges, OnDestroy {
    private cdr;
    private ngZone;
    /** @override */
    nzFormat: string;
    diff: number;
    private target;
    private updater_;
    constructor(cdr: ChangeDetectorRef, ngZone: NgZone);
    /** @override */
    ngOnChanges(changes: SimpleChanges): void;
    ngOnInit(): void;
    ngOnDestroy(): void;
    syncTimer(): void;
    startTimer(): void;
    stopTimer(): void;
    /**
     * Update time that should be displayed on the screen.
     */
    protected updateValue(): void;
}
