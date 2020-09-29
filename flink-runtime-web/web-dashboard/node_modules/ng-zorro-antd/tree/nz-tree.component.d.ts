import { ChangeDetectorRef, EventEmitter, OnChanges, OnDestroy, OnInit, SimpleChange, TemplateRef } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';
import { Observable, ReplaySubject, Subject } from 'rxjs';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzTreeSelectService } from '../tree-select/nz-tree-select.service';
import { NzFormatBeforeDropEvent, NzFormatEmitEvent } from '../tree/interface';
import { NzTreeBaseService } from './nz-tree-base.service';
import { NzTreeNode } from './nz-tree-node';
import { NzTreeService } from './nz-tree.service';
export declare function NzTreeServiceFactory(treeSelectService: NzTreeSelectService, treeService: NzTreeService): NzTreeBaseService;
export declare class NzTreeComponent implements OnInit, OnDestroy, ControlValueAccessor, OnChanges {
    nzTreeService: NzTreeBaseService;
    private cdr;
    noAnimation?: NzNoAnimationDirective | undefined;
    nzShowIcon: boolean;
    nzShowLine: boolean;
    nzCheckable: boolean;
    nzShowExpand: boolean;
    nzAsyncData: boolean;
    nzDraggable: boolean;
    nzExpandAll: boolean;
    nzHideUnMatched: boolean;
    nzSelectMode: boolean;
    nzCheckStrictly: boolean;
    /**
     * @deprecated use
     * nzExpandAll instead
     */
    nzDefaultExpandAll: boolean;
    nzBeforeDrop: (confirm: NzFormatBeforeDropEvent) => Observable<boolean>;
    nzMultiple: boolean;
    nzData: any[];
    /**
     * @deprecated use
     * nzExpandedKeys instead
     */
    nzDefaultExpandedKeys: string[];
    /**
     * @deprecated use
     * nzSelectedKeys instead
     */
    nzDefaultSelectedKeys: string[];
    /**
     * @deprecated use
     * nzCheckedKeys instead
     */
    nzDefaultCheckedKeys: string[];
    nzExpandedKeys: string[];
    nzSelectedKeys: string[];
    nzCheckedKeys: string[];
    nzSearchValue: string;
    readonly nzExpandedKeysChange: EventEmitter<string[]>;
    readonly nzSelectedKeysChange: EventEmitter<string[]>;
    readonly nzCheckedKeysChange: EventEmitter<string[]>;
    readonly nzSearchValueChange: EventEmitter<NzFormatEmitEvent>;
    /**
     * @deprecated use
     * nzSearchValueChange instead
     */
    readonly nzOnSearchNode: EventEmitter<NzFormatEmitEvent>;
    readonly nzClick: EventEmitter<NzFormatEmitEvent>;
    readonly nzDblClick: EventEmitter<NzFormatEmitEvent>;
    readonly nzContextMenu: EventEmitter<NzFormatEmitEvent>;
    readonly nzCheckBoxChange: EventEmitter<NzFormatEmitEvent>;
    readonly nzExpandChange: EventEmitter<NzFormatEmitEvent>;
    readonly nzOnDragStart: EventEmitter<NzFormatEmitEvent>;
    readonly nzOnDragEnter: EventEmitter<NzFormatEmitEvent>;
    readonly nzOnDragOver: EventEmitter<NzFormatEmitEvent>;
    readonly nzOnDragLeave: EventEmitter<NzFormatEmitEvent>;
    readonly nzOnDrop: EventEmitter<NzFormatEmitEvent>;
    readonly nzOnDragEnd: EventEmitter<NzFormatEmitEvent>;
    nzTreeTemplate: TemplateRef<any>;
    _searchValue: string;
    _nzMultiple: boolean;
    nzDefaultSubject: ReplaySubject<{
        type: string;
        keys: string[];
    }>;
    destroy$: Subject<{}>;
    nzNodes: NzTreeNode[];
    prefixCls: string;
    classMap: {};
    onChange: (value: NzTreeNode[]) => void;
    onTouched: () => void;
    getTreeNodes(): NzTreeNode[];
    getTreeNodeByKey(key: string): NzTreeNode | null;
    /**
     * public function
     */
    getCheckedNodeList(): NzTreeNode[];
    getSelectedNodeList(): NzTreeNode[];
    getHalfCheckedNodeList(): NzTreeNode[];
    getExpandedNodeList(): NzTreeNode[];
    getMatchedNodeList(): NzTreeNode[];
    setClassMap(): void;
    writeValue(value: NzTreeNode[]): void;
    registerOnChange(fn: (_: NzTreeNode[]) => void): void;
    registerOnTouched(fn: () => void): void;
    initNzData(value: any[]): void;
    constructor(nzTreeService: NzTreeBaseService, cdr: ChangeDetectorRef, noAnimation?: NzNoAnimationDirective | undefined);
    ngOnInit(): void;
    ngOnChanges(changes: {
        [propertyName: string]: SimpleChange;
    }): void;
    ngOnDestroy(): void;
}
