import { ElementRef, OnChanges, OnDestroy, OnInit, Renderer2, SimpleChanges } from '@angular/core';
import { Subject } from 'rxjs';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzMenuService } from './nz-menu.service';
import { NzSubmenuService } from './nz-submenu.service';
export declare class NzMenuItemDirective implements OnInit, OnChanges, OnDestroy {
    private nzUpdateHostClassService;
    private nzMenuService;
    private nzSubmenuService;
    private renderer;
    private elementRef;
    private el;
    private destroy$;
    private originalPadding;
    selected$: Subject<boolean>;
    nzPaddingLeft: number;
    nzDisabled: boolean;
    nzSelected: boolean;
    /** clear all item selected status except this */
    clickMenuItem(e: MouseEvent): void;
    setClassMap(): void;
    setSelectedState(value: boolean): void;
    constructor(nzUpdateHostClassService: NzUpdateHostClassService, nzMenuService: NzMenuService, nzSubmenuService: NzSubmenuService, renderer: Renderer2, elementRef: ElementRef);
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    ngOnDestroy(): void;
}
