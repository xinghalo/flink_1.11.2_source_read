import { ChangeDetectorRef, EventEmitter, OnDestroy, OnInit, TemplateRef } from '@angular/core';
import { NzScrollService } from '../core/scroll/nz-scroll.service';
export declare class NzBackTopComponent implements OnInit, OnDestroy {
    private scrollSrv;
    private doc;
    private cd;
    private scroll$;
    private target;
    visible: boolean;
    nzTemplate: TemplateRef<void>;
    private _visibilityHeight;
    nzVisibilityHeight: number;
    nzTarget: string | HTMLElement;
    readonly nzClick: EventEmitter<boolean>;
    constructor(scrollSrv: NzScrollService, doc: any, cd: ChangeDetectorRef);
    ngOnInit(): void;
    clickBackTop(): void;
    private getTarget;
    private handleScroll;
    private removeListen;
    private registerScrollEvent;
    ngOnDestroy(): void;
}
