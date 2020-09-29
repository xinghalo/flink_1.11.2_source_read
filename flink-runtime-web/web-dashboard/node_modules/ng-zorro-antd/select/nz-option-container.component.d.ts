import { ChangeDetectorRef, ElementRef, EventEmitter, NgZone, OnDestroy, OnInit, QueryList, TemplateRef } from '@angular/core';
import { NzOptionGroupComponent } from './nz-option-group.component';
import { NzOptionLiComponent } from './nz-option-li.component';
import { NzOptionComponent } from './nz-option.component';
import { NzSelectService } from './nz-select.service';
export declare class NzOptionContainerComponent implements OnDestroy, OnInit {
    nzSelectService: NzSelectService;
    private cdr;
    private ngZone;
    private destroy$;
    listOfNzOptionLiComponent: QueryList<NzOptionLiComponent>;
    dropdownUl: ElementRef;
    nzNotFoundContent: string;
    nzMenuItemSelectedIcon: TemplateRef<void>;
    readonly nzScrollToBottom: EventEmitter<void>;
    scrollIntoViewIfNeeded(option: NzOptionComponent): void;
    trackLabel(_index: number, option: NzOptionGroupComponent): string | TemplateRef<void>;
    trackValue(_index: number, option: NzOptionComponent): any;
    constructor(nzSelectService: NzSelectService, cdr: ChangeDetectorRef, ngZone: NgZone);
    ngOnInit(): void;
    ngOnDestroy(): void;
}
