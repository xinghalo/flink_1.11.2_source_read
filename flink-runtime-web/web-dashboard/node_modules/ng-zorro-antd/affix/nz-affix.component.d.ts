import { ElementRef, EventEmitter, OnDestroy, OnInit } from '@angular/core';
import { NzScrollService } from '../core/scroll/nz-scroll.service';
export declare class NzAffixComponent implements OnInit, OnDestroy {
    private scrollSrv;
    private doc;
    nzTarget: string | Element | Window;
    nzOffsetTop: number | null;
    nzOffsetBottom: number;
    readonly nzChange: EventEmitter<boolean>;
    private timeout;
    private readonly events;
    private fixedEl;
    private readonly placeholderNode;
    private affixStyle;
    private placeholderStyle;
    private _target;
    private _offsetTop;
    private _offsetBottom;
    constructor(_el: ElementRef, scrollSrv: NzScrollService, doc: any);
    ngOnInit(): void;
    ngOnDestroy(): void;
    getOffset(element: Element, target: Element | Window | undefined): {
        top: number;
        left: number;
        width: number;
        height: number;
    };
    private setTargetEventListeners;
    private clearEventListeners;
    private getTargetRect;
    private genStyle;
    private setAffixStyle;
    private setPlaceholderStyle;
    private syncPlaceholderStyle;
    updatePosition(e: Event): void;
}
