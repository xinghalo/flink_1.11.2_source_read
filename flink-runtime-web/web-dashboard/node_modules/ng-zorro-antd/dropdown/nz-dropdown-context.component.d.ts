import { ConnectedOverlayPositionChange } from '@angular/cdk/overlay';
import { ChangeDetectorRef, OnDestroy, TemplateRef } from '@angular/core';
import { Observable } from 'rxjs';
import { NzDropdownService } from './nz-dropdown.service';
export declare class NzDropdownContextComponent implements OnDestroy {
    private cdr;
    open: boolean;
    templateRef: TemplateRef<void>;
    dropDownPosition: 'top' | 'bottom';
    private control;
    private destroy$;
    init(open: boolean, templateRef: TemplateRef<void>, positionChanges: Observable<ConnectedOverlayPositionChange>, control: NzDropdownService): void;
    close(): void;
    afterAnimation(): void;
    constructor(cdr: ChangeDetectorRef);
    /** https://github.com/angular/angular/issues/14842 **/
    ngOnDestroy(): void;
}
