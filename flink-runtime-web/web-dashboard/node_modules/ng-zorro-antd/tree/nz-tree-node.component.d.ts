import { ChangeDetectorRef, ElementRef, NgZone, OnChanges, OnDestroy, OnInit, Renderer2, TemplateRef } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzFormatBeforeDropEvent } from '../tree/interface';
import { NzTreeBaseService } from './nz-tree-base.service';
import { NzTreeNode } from './nz-tree-node';
export declare class NzTreeNodeComponent implements OnInit, OnChanges, OnDestroy {
    nzTreeService: NzTreeBaseService;
    private ngZone;
    private renderer;
    private elRef;
    private cdr;
    noAnimation?: NzNoAnimationDirective | undefined;
    dragElement: ElementRef;
    /**
     * for global property
     */
    nzTreeNode: NzTreeNode;
    nzShowLine: boolean;
    nzShowExpand: boolean;
    nzCheckable: boolean;
    nzAsyncData: boolean;
    nzHideUnMatched: boolean;
    nzNoAnimation: boolean;
    nzSelectMode: boolean;
    nzShowIcon: boolean;
    nzTreeTemplate: TemplateRef<void>;
    nzBeforeDrop: (confirm: NzFormatBeforeDropEvent) => Observable<boolean>;
    nzDraggable: boolean;
    /**
     * @deprecated use
     * nzExpandAll instead
     */
    nzDefaultExpandAll: boolean;
    nzExpandAll: boolean;
    nzSearchValue: string;
    prefixCls: string;
    highlightKeys: string[];
    nzNodeClass: {};
    nzNodeSwitcherClass: {};
    nzNodeContentClass: {};
    nzNodeCheckboxClass: {};
    nzNodeContentIconClass: {};
    nzNodeContentLoadingClass: {};
    /**
     * drag var
     */
    destroy$: Subject<{}>;
    dragPos: number;
    dragPosClass: {
        [key: string]: string;
    };
    /**
     * default set
     */
    _searchValue: string;
    _nzDraggable: boolean;
    _nzExpandAll: boolean;
    readonly nzIcon: string;
    readonly canDraggable: boolean | null;
    readonly isShowLineIcon: boolean;
    readonly isShowSwitchIcon: boolean;
    readonly isSwitcherOpen: boolean;
    readonly isSwitcherClose: boolean;
    readonly displayStyle: string;
    /**
     * reset node class
     */
    setClassMap(): void;
    onMousedown(event: MouseEvent): void;
    /**
     * click node to select, 200ms to dbl click
     */
    nzClick(event: MouseEvent): void;
    nzDblClick(event: MouseEvent): void;
    /**
     * @param event
     */
    nzContextMenu(event: MouseEvent): void;
    /**
     * collapse node
     * @param event
     */
    _clickExpand(event: MouseEvent): void;
    /**
     * check node
     * @param event
     */
    _clickCheckBox(event: MouseEvent): void;
    /**
     * drag event
     * @param e
     */
    clearDragClass(): void;
    handleDragStart(e: DragEvent): void;
    handleDragEnter(e: DragEvent): void;
    handleDragOver(e: DragEvent): void;
    handleDragLeave(e: DragEvent): void;
    handleDragDrop(e: DragEvent): void;
    handleDragEnd(e: DragEvent): void;
    /**
     * 监听拖拽事件
     */
    handDragEvent(): void;
    markForCheck(): void;
    constructor(nzTreeService: NzTreeBaseService, ngZone: NgZone, renderer: Renderer2, elRef: ElementRef, cdr: ChangeDetectorRef, noAnimation?: NzNoAnimationDirective | undefined);
    ngOnInit(): void;
    ngOnChanges(): void;
    ngOnDestroy(): void;
}
