import { AfterContentInit, ChangeDetectorRef, EventEmitter, OnChanges, OnDestroy } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzDropDownComponent } from './nz-dropdown.component';
import { NzDropDownDirective } from './nz-dropdown.directive';
import { NzMenuDropdownService } from './nz-menu-dropdown.service';
export declare class NzDropDownButtonComponent extends NzDropDownComponent implements OnDestroy, AfterContentInit, OnChanges {
    noAnimation?: NzNoAnimationDirective | undefined;
    nzSize: string;
    nzType: string;
    readonly nzClick: EventEmitter<MouseEvent>;
    nzDropDownDirective: NzDropDownDirective;
    constructor(cdr: ChangeDetectorRef, nzMenuDropdownService: NzMenuDropdownService, noAnimation?: NzNoAnimationDirective | undefined);
    /** rewrite afterViewInit hook */
    ngAfterContentInit(): void;
}
