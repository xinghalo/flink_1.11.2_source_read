/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { BACKSPACE } from '@angular/cdk/keycodes';
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { forwardRef, ChangeDetectorRef, Component, ElementRef, EventEmitter, Host, Input, Optional, Output, Renderer2, TemplateRef, ViewChild } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { merge, of as observableOf } from 'rxjs';
import { filter, tap } from 'rxjs/operators';
import { slideMotion } from '../core/animation/slide';
import { zoomMotion } from '../core/animation/zoom';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { InputBoolean } from '../core/util/convert';
import { NzTreeNode } from '../tree/nz-tree-node';
import { NzTreeComponent } from '../tree/nz-tree.component';
import { NzTreeSelectService } from './nz-tree-select.service';
var NzTreeSelectComponent = /** @class */ (function () {
    function NzTreeSelectComponent(renderer, cdr, nzTreeService, elementRef, noAnimation) {
        this.renderer = renderer;
        this.cdr = cdr;
        this.nzTreeService = nzTreeService;
        this.elementRef = elementRef;
        this.noAnimation = noAnimation;
        this.nzAllowClear = true;
        this.nzShowExpand = true;
        this.nzDropdownMatchSelectWidth = true;
        this.nzCheckable = false;
        this.nzShowSearch = false;
        this.nzDisabled = false;
        this.nzShowLine = false;
        this.nzAsyncData = false;
        this.nzMultiple = false;
        this.nzDefaultExpandAll = false;
        this.nzNodes = [];
        this.nzOpen = false;
        this.nzSize = 'default';
        this.nzPlaceHolder = '';
        this.nzDefaultExpandedKeys = [];
        this.nzDisplayWith = (/**
         * @param {?} node
         * @return {?}
         */
        function (node) { return node.title; });
        this.nzOpenChange = new EventEmitter();
        this.nzCleared = new EventEmitter();
        this.nzRemoved = new EventEmitter();
        this.nzExpandChange = new EventEmitter();
        this.nzTreeClick = new EventEmitter();
        this.nzTreeCheckBoxChange = new EventEmitter();
        this.isComposing = false;
        this.isDestroy = true;
        this.isNotFound = false;
        this.inputValue = '';
        this.dropDownPosition = 'bottom';
        this.selectedNodes = [];
        this.value = [];
        this.onTouched = (/**
         * @return {?}
         */
        function () { return null; });
        this.renderer.addClass(this.elementRef.nativeElement, 'ant-select');
    }
    Object.defineProperty(NzTreeSelectComponent.prototype, "placeHolderDisplay", {
        get: /**
         * @return {?}
         */
        function () {
            return this.inputValue || this.isComposing || this.selectedNodes.length ? 'none' : 'block';
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeSelectComponent.prototype, "searchDisplay", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzOpen ? 'block' : 'none';
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeSelectComponent.prototype, "isMultiple", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzMultiple || this.nzCheckable;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeSelectComponent.prototype, "selectedValueDisplay", {
        get: /**
         * @return {?}
         */
        function () {
            /** @type {?} */
            var showSelectedValue = false;
            /** @type {?} */
            var opacity = 1;
            if (!this.nzShowSearch) {
                showSelectedValue = true;
            }
            else {
                if (this.nzOpen) {
                    showSelectedValue = !(this.inputValue || this.isComposing);
                    if (showSelectedValue) {
                        opacity = 0.4;
                    }
                }
                else {
                    showSelectedValue = true;
                }
            }
            return {
                display: showSelectedValue ? 'block' : 'none',
                opacity: "" + opacity
            };
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.isDestroy = false;
        this.selectionChangeSubscription = this.subscribeSelectionChange();
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.isDestroy = true;
        this.closeDropDown();
        this.selectionChangeSubscription.unsubscribe();
    };
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    NzTreeSelectComponent.prototype.setDisabledState = /**
     * @param {?} isDisabled
     * @return {?}
     */
    function (isDisabled) {
        this.nzDisabled = isDisabled;
        this.closeDropDown();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzTreeSelectComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.hasOwnProperty('nzNodes')) {
            this.updateSelectedNodes(true);
        }
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzTreeSelectComponent.prototype.writeValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        var _this = this;
        if (value) {
            if (this.isMultiple && Array.isArray(value)) {
                this.value = value;
            }
            else {
                this.value = [(/** @type {?} */ (value))];
            }
            this.updateSelectedNodes(true);
        }
        else {
            this.value = [];
            this.selectedNodes.forEach((/**
             * @param {?} node
             * @return {?}
             */
            function (node) {
                _this.removeSelected(node, false);
            }));
            this.selectedNodes = [];
        }
        this.cdr.markForCheck();
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzTreeSelectComponent.prototype.registerOnChange = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onChange = fn;
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzTreeSelectComponent.prototype.registerOnTouched = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onTouched = fn;
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.trigger = /**
     * @return {?}
     */
    function () {
        if (this.nzDisabled || (!this.nzDisabled && this.nzOpen)) {
            this.closeDropDown();
        }
        else {
            this.openDropdown();
            if (this.nzShowSearch || this.isMultiple) {
                this.focusOnInput();
            }
        }
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.openDropdown = /**
     * @return {?}
     */
    function () {
        if (!this.nzDisabled) {
            this.nzOpen = true;
            this.nzOpenChange.emit(this.nzOpen);
            this.updateCdkConnectedOverlayStatus();
            this.updatePosition();
        }
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.closeDropDown = /**
     * @return {?}
     */
    function () {
        this.onTouched();
        this.nzOpen = false;
        this.nzOpenChange.emit(this.nzOpen);
        this.cdr.markForCheck();
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzTreeSelectComponent.prototype.onKeyDownInput = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        /** @type {?} */
        var keyCode = e.keyCode;
        /** @type {?} */
        var eventTarget = (/** @type {?} */ (e.target));
        if (this.isMultiple && !eventTarget.value && keyCode === BACKSPACE) {
            e.preventDefault();
            if (this.selectedNodes.length) {
                /** @type {?} */
                var removeNode = this.selectedNodes[this.selectedNodes.length - 1];
                this.removeSelected(removeNode);
                (/** @type {?} */ ((/** @type {?} */ (this.nzTreeService)).triggerEventChange$)).next({
                    eventName: 'removeSelect',
                    node: removeNode
                });
            }
        }
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzTreeSelectComponent.prototype.onExpandedKeysChange = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.nzExpandChange.emit(value);
        this.nzDefaultExpandedKeys = tslib_1.__spread((/** @type {?} */ (value.keys)));
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzTreeSelectComponent.prototype.setInputValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.inputValue = value;
        this.updateInputWidth();
        this.updatePosition();
    };
    /**
     * @param {?} node
     * @param {?=} emit
     * @param {?=} event
     * @return {?}
     */
    NzTreeSelectComponent.prototype.removeSelected = /**
     * @param {?} node
     * @param {?=} emit
     * @param {?=} event
     * @return {?}
     */
    function (node, emit, event) {
        if (emit === void 0) { emit = true; }
        node.isSelected = false;
        node.isChecked = false;
        if (this.nzCheckable) {
            this.nzTreeService.conduct(node);
        }
        else {
            this.nzTreeService.setSelectedNodeList(node, this.nzMultiple);
        }
        if (emit) {
            this.nzRemoved.emit(node);
        }
        // Do not trigger the popup
        if (event && event.stopPropagation) {
            event.stopPropagation();
        }
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.focusOnInput = /**
     * @return {?}
     */
    function () {
        var _this = this;
        setTimeout((/**
         * @return {?}
         */
        function () {
            if (_this.inputElement) {
                _this.inputElement.nativeElement.focus();
            }
        }));
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.subscribeSelectionChange = /**
     * @return {?}
     */
    function () {
        var _this = this;
        return merge(this.nzTreeClick.pipe(tap((/**
         * @param {?} event
         * @return {?}
         */
        function (event) {
            /** @type {?} */
            var node = (/** @type {?} */ (event.node));
            if (_this.nzCheckable && !node.isDisabled && !node.isDisableCheckbox) {
                node.isChecked = !node.isChecked;
                _this.nzTreeService.conduct(node);
            }
            if (_this.nzCheckable) {
                node.isSelected = false;
            }
        })), filter((/**
         * @param {?} event
         * @return {?}
         */
        function (event) {
            /** @type {?} */
            var node = (/** @type {?} */ (event.node));
            return _this.nzCheckable ? !node.isDisabled && !node.isDisableCheckbox : !node.isDisabled;
        }))), this.nzCheckable ? this.nzTreeCheckBoxChange : observableOf(), this.nzCleared, this.nzRemoved).subscribe((/**
         * @return {?}
         */
        function () {
            _this.updateSelectedNodes();
            /** @type {?} */
            var value = _this.selectedNodes.map((/**
             * @param {?} node
             * @return {?}
             */
            function (node) { return (/** @type {?} */ (node.key)); }));
            _this.value = tslib_1.__spread(value);
            if (_this.nzShowSearch || _this.isMultiple) {
                _this.inputValue = '';
                _this.isNotFound = false;
            }
            if (_this.isMultiple) {
                _this.onChange(value);
                _this.focusOnInput();
                _this.updatePosition();
            }
            else {
                _this.closeDropDown();
                _this.onChange(value.length ? value[0] : null);
            }
        }));
    };
    /**
     * @param {?=} init
     * @return {?}
     */
    NzTreeSelectComponent.prototype.updateSelectedNodes = /**
     * @param {?=} init
     * @return {?}
     */
    function (init) {
        var _this = this;
        if (init === void 0) { init = false; }
        if (init) {
            /** @type {?} */
            var nodes = void 0;
            this.nzTreeService.isMultiple = this.isMultiple;
            if (!this.nzTreeService.isArrayOfNzTreeNode(this.nzNodes)) {
                // has not been new NzTreeNode
                nodes = this.nzNodes.map((/**
                 * @param {?} item
                 * @return {?}
                 */
                function (item) { return new NzTreeNode(item, undefined, _this.nzTreeService); }));
            }
            else {
                nodes = this.nzNodes.map((/**
                 * @param {?} item
                 * @return {?}
                 */
                function (item) { return new NzTreeNode(tslib_1.__assign({}, item.origin), undefined, _this.nzTreeService); }));
            }
            this.nzTreeService.initTree(nodes);
            if (this.nzCheckable) {
                this.nzTreeService.calcCheckedKeys(this.value, nodes);
            }
            else {
                this.nzTreeService.calcSelectedKeys(this.value, nodes, this.isMultiple);
            }
        }
        this.selectedNodes = tslib_1.__spread((this.nzCheckable ? this.nzTreeService.getCheckedNodeList() : this.nzTreeService.getSelectedNodeList()));
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.updatePosition = /**
     * @return {?}
     */
    function () {
        var _this = this;
        setTimeout((/**
         * @return {?}
         */
        function () {
            if (_this.cdkConnectedOverlay && _this.cdkConnectedOverlay.overlayRef) {
                _this.cdkConnectedOverlay.overlayRef.updatePosition();
            }
        }));
    };
    /**
     * @param {?} position
     * @return {?}
     */
    NzTreeSelectComponent.prototype.onPositionChange = /**
     * @param {?} position
     * @return {?}
     */
    function (position) {
        this.dropDownPosition = position.connectionPair.originY;
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.updateInputWidth = /**
     * @return {?}
     */
    function () {
        if (this.isMultiple && this.inputElement) {
            if (this.inputValue || this.isComposing) {
                this.renderer.setStyle(this.inputElement.nativeElement, 'width', this.inputElement.nativeElement.scrollWidth + "px");
            }
            else {
                this.renderer.removeStyle(this.inputElement.nativeElement, 'width');
            }
        }
    };
    /**
     * @param {?} $event
     * @return {?}
     */
    NzTreeSelectComponent.prototype.onClearSelection = /**
     * @param {?} $event
     * @return {?}
     */
    function ($event) {
        var _this = this;
        $event.stopPropagation();
        $event.preventDefault();
        this.selectedNodes.forEach((/**
         * @param {?} node
         * @return {?}
         */
        function (node) {
            _this.removeSelected(node, false);
        }));
        this.nzCleared.emit();
    };
    /**
     * @param {?} $event
     * @return {?}
     */
    NzTreeSelectComponent.prototype.setSearchValues = /**
     * @param {?} $event
     * @return {?}
     */
    function ($event) {
        var _this = this;
        Promise.resolve().then((/**
         * @return {?}
         */
        function () {
            _this.isNotFound = (_this.nzShowSearch || _this.isMultiple) && !!_this.inputValue && (/** @type {?} */ ($event.matchedKeys)).length === 0;
        }));
    };
    /**
     * @return {?}
     */
    NzTreeSelectComponent.prototype.updateCdkConnectedOverlayStatus = /**
     * @return {?}
     */
    function () {
        this.triggerWidth = this.cdkOverlayOrigin.elementRef.nativeElement.getBoundingClientRect().width;
    };
    /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    NzTreeSelectComponent.prototype.trackValue = /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    function (_index, option) {
        return (/** @type {?} */ (option.key));
    };
    NzTreeSelectComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-tree-select',
                    animations: [slideMotion, zoomMotion],
                    template: "<ng-template #inputTemplate>\n  <input\n    #inputElement\n    autocomplete=\"off\"\n    class=\"ant-select-search__field\"\n    (compositionstart)=\"isComposing = true\"\n    (compositionend)=\"isComposing = false\"\n    (keydown)=\"onKeyDownInput($event)\"\n    [ngModel]=\"inputValue\"\n    (ngModelChange)=\"setInputValue($event)\"\n    [disabled]=\"nzDisabled\">\n</ng-template>\n\n<ng-template\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  [cdkConnectedOverlayOrigin]=\"cdkOverlayOrigin\"\n  [cdkConnectedOverlayOpen]=\"nzOpen\"\n  [cdkConnectedOverlayHasBackdrop]=\"true\"\n  [cdkConnectedOverlayMinWidth]=\"nzDropdownMatchSelectWidth? null : triggerWidth\"\n  [cdkConnectedOverlayWidth]=\"nzDropdownMatchSelectWidth? triggerWidth : null\"\n  (backdropClick)=\"closeDropDown()\"\n  (detach)=\"closeDropDown()\"\n  (positionChange)=\"onPositionChange($event)\">\n  <div class=\"ant-select-dropdown ant-select-tree-dropdown\"\n    [@slideMotion]=\"nzOpen ? dropDownPosition : 'void'\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [class.ant-select-dropdown--single]=\"!nzMultiple\"\n    [class.ant-select-dropdown--multiple]=\"nzMultiple\"\n    [class.ant-select-dropdown-placement-bottomLeft]=\"dropDownPosition === 'bottom'\"\n    [class.ant-select-dropdown-placement-topLeft]=\"dropDownPosition === 'top'\"\n    [ngStyle]=\"nzDropdownStyle\">\n    <nz-tree\n      #treeRef\n      [hidden]=\"isNotFound\"\n      nzNoAnimation\n      nzSelectMode\n      [nzData]=\"nzNodes\"\n      [nzMultiple]=\"nzMultiple\"\n      [nzSearchValue]=\"inputValue\"\n      [nzCheckable]=\"nzCheckable\"\n      [nzAsyncData]=\"nzAsyncData\"\n      [nzShowExpand]=\"nzShowExpand\"\n      [nzShowLine]=\"nzShowLine\"\n      [nzExpandAll]=\"nzDefaultExpandAll\"\n      [nzExpandedKeys]=\"nzDefaultExpandedKeys\"\n      [nzCheckedKeys]=\"nzCheckable ? value : []\"\n      [nzSelectedKeys]=\"!nzCheckable ? value : []\"\n      (nzExpandChange)=\"onExpandedKeysChange($event)\"\n      (nzClick)=\"nzTreeClick.emit($event)\"\n      (nzCheckedKeysChange)=\"updateSelectedNodes()\"\n      (nzSelectedKeysChange)=\"updateSelectedNodes()\"\n      (nzCheckBoxChange)=\"nzTreeCheckBoxChange.emit($event)\"\n      (nzSearchValueChange)=\"setSearchValues($event)\">\n    </nz-tree>\n    <span *ngIf=\"nzNodes.length === 0 || isNotFound\" class=\"ant-select-not-found\">\n      <nz-embed-empty [nzComponentName]=\"'tree-select'\" [specificContent]=\"nzNotFoundContent\"></nz-embed-empty>\n    </span>\n  </div>\n</ng-template>\n\n<div\n  cdkOverlayOrigin\n  class=\"ant-select-selection\"\n  [class.ant-select-selection--single]=\"!isMultiple\"\n  [class.ant-select-selection--multiple]=\"isMultiple\"\n  tabindex=\"0\">\n  <ng-container *ngIf=\"!isMultiple\">\n    <div class=\"ant-select-selection__rendered\">\n      <div\n        *ngIf=\"nzPlaceHolder && selectedNodes.length === 0\"\n        [style.display]=\"placeHolderDisplay\"\n        class=\"ant-select-selection__placeholder\">\n        {{ nzPlaceHolder }}\n      </div>\n\n      <div\n        *ngIf=\"selectedNodes.length === 1\"\n        class=\"ant-select-selection-selected-value\"\n        [attr.title]=\"nzDisplayWith(selectedNodes[0])\"\n        [ngStyle]=\"selectedValueDisplay\">\n        {{ nzDisplayWith(selectedNodes[0]) }}\n      </div>\n\n      <div\n        *ngIf=\"nzShowSearch\"\n        [style.display]=\"searchDisplay\"\n        class=\"ant-select-search ant-select-search--inline\">\n        <div class=\"ant-select-search__field__wrap\">\n          <ng-template [ngTemplateOutlet]=\"inputTemplate\"></ng-template>\n          <span class=\"ant-select-search__field__mirror\">{{inputValue}}&nbsp;</span>\n        </div>\n      </div>\n\n    </div>\n  </ng-container>\n  <ng-container *ngIf=\"isMultiple\">\n    <ul class=\"ant-select-selection__rendered\">\n      <div\n        *ngIf=\"nzPlaceHolder && selectedNodes.length === 0\"\n        [style.display]=\"placeHolderDisplay\"\n        class=\"ant-select-selection__placeholder\">\n        {{ nzPlaceHolder }}\n      </div>\n      <ng-container *ngFor=\"let node of selectedNodes | slice: 0 : nzMaxTagCount; trackBy:trackValue\">\n        <li\n          [@zoomMotion]\n          [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n          [attr.title]=\"nzDisplayWith(node)\"\n          [class.ant-select-selection__choice__disabled]=\"node.isDisabled\"\n          class=\"ant-select-selection__choice\">\n               <span *ngIf=\"!node.isDisabled\" class=\"ant-select-selection__choice__remove\"\n                 (mousedown)=\"$event.preventDefault()\"\n                 (click)=\"removeSelected(node, true, $event)\">\n                 <i nz-icon type=\"close\" class=\"ant-select-remove-icon\"></i>\n               </span>\n          <span class=\"ant-select-selection__choice__content\">{{ nzDisplayWith(node) }}</span>\n        </li>\n      </ng-container>\n      <li [@zoomMotion]\n        *ngIf=\"selectedNodes.length > nzMaxTagCount\"\n        class=\"ant-select-selection__choice\">\n        <div class=\"ant-select-selection__choice__content\">\n          <ng-container *ngIf=\"nzMaxTagPlaceholder\">\n            <ng-template\n              [ngTemplateOutlet]=\"nzMaxTagPlaceholder\"\n              [ngTemplateOutletContext]=\"{ $implicit: selectedNodes | slice: nzMaxTagCount}\">\n            </ng-template>\n          </ng-container>\n          <ng-container *ngIf=\"!nzMaxTagPlaceholder\">\n            + {{ selectedNodes.length - nzMaxTagCount }} ...\n          </ng-container>\n        </div>\n      </li>\n      <li class=\"ant-select-search ant-select-search--inline\">\n        <ng-template [ngTemplateOutlet]=\"inputTemplate\"></ng-template>\n      </li>\n    </ul>\n  </ng-container>\n  <span *ngIf=\"nzAllowClear\" class=\"ant-select-selection__clear\"\n    (mousedown)=\"$event.preventDefault()\"\n    (click)=\"onClearSelection($event)\">\n    <i nz-icon type=\"close-circle\" class=\"ant-select-clear-icon\" theme=\"fill\"></i>\n  </span>\n  <span *ngIf=\"!isMultiple\" class=\"ant-select-arrow\">\n    <i nz-icon type=\"down\" class=\"ant-select-arrow-icon\"></i>\n  </span>\n</div>",
                    providers: [
                        NzTreeSelectService,
                        {
                            provide: NG_VALUE_ACCESSOR,
                            useExisting: forwardRef((/**
                             * @return {?}
                             */
                            function () { return NzTreeSelectComponent; })),
                            multi: true
                        }
                    ],
                    host: {
                        '[class.ant-select-lg]': 'nzSize==="large"',
                        '[class.ant-select-sm]': 'nzSize==="small"',
                        '[class.ant-select-enabled]': '!nzDisabled',
                        '[class.ant-select-disabled]': 'nzDisabled',
                        '[class.ant-select-allow-clear]': 'nzAllowClear',
                        '[class.ant-select-open]': 'nzOpen',
                        '(click)': 'trigger()'
                    },
                    styles: ["\n      .ant-select-dropdown {\n        top: 100%;\n        left: 0;\n        position: relative;\n        width: 100%;\n        margin-top: 4px;\n        margin-bottom: 4px;\n        overflow: auto;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzTreeSelectComponent.ctorParameters = function () { return [
        { type: Renderer2 },
        { type: ChangeDetectorRef },
        { type: NzTreeSelectService },
        { type: ElementRef },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzTreeSelectComponent.propDecorators = {
        nzAllowClear: [{ type: Input }],
        nzShowExpand: [{ type: Input }],
        nzDropdownMatchSelectWidth: [{ type: Input }],
        nzCheckable: [{ type: Input }],
        nzShowSearch: [{ type: Input }],
        nzDisabled: [{ type: Input }],
        nzShowLine: [{ type: Input }],
        nzAsyncData: [{ type: Input }],
        nzMultiple: [{ type: Input }],
        nzDefaultExpandAll: [{ type: Input }],
        nzNotFoundContent: [{ type: Input }],
        nzNodes: [{ type: Input }],
        nzOpen: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzPlaceHolder: [{ type: Input }],
        nzDropdownStyle: [{ type: Input }],
        nzDefaultExpandedKeys: [{ type: Input }],
        nzDisplayWith: [{ type: Input }],
        nzMaxTagCount: [{ type: Input }],
        nzMaxTagPlaceholder: [{ type: Input }],
        nzOpenChange: [{ type: Output }],
        nzCleared: [{ type: Output }],
        nzRemoved: [{ type: Output }],
        nzExpandChange: [{ type: Output }],
        nzTreeClick: [{ type: Output }],
        nzTreeCheckBoxChange: [{ type: Output }],
        inputElement: [{ type: ViewChild, args: ['inputElement',] }],
        treeRef: [{ type: ViewChild, args: ['treeRef',] }],
        cdkOverlayOrigin: [{ type: ViewChild, args: [CdkOverlayOrigin,] }],
        cdkConnectedOverlay: [{ type: ViewChild, args: [CdkConnectedOverlay,] }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzAllowClear", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzShowExpand", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzDropdownMatchSelectWidth", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzCheckable", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzShowSearch", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzDisabled", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzShowLine", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzAsyncData", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzMultiple", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTreeSelectComponent.prototype, "nzDefaultExpandAll", void 0);
    return NzTreeSelectComponent;
}());
export { NzTreeSelectComponent };
if (false) {
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzAllowClear;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzShowExpand;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzDropdownMatchSelectWidth;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzCheckable;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzShowSearch;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzDisabled;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzShowLine;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzAsyncData;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzMultiple;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzDefaultExpandAll;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzNotFoundContent;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzNodes;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzOpen;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzSize;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzPlaceHolder;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzDropdownStyle;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzDefaultExpandedKeys;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzDisplayWith;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzMaxTagCount;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzMaxTagPlaceholder;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzOpenChange;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzCleared;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzRemoved;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzExpandChange;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzTreeClick;
    /** @type {?} */
    NzTreeSelectComponent.prototype.nzTreeCheckBoxChange;
    /** @type {?} */
    NzTreeSelectComponent.prototype.inputElement;
    /** @type {?} */
    NzTreeSelectComponent.prototype.treeRef;
    /** @type {?} */
    NzTreeSelectComponent.prototype.cdkOverlayOrigin;
    /** @type {?} */
    NzTreeSelectComponent.prototype.cdkConnectedOverlay;
    /** @type {?} */
    NzTreeSelectComponent.prototype.triggerWidth;
    /** @type {?} */
    NzTreeSelectComponent.prototype.isComposing;
    /** @type {?} */
    NzTreeSelectComponent.prototype.isDestroy;
    /** @type {?} */
    NzTreeSelectComponent.prototype.isNotFound;
    /** @type {?} */
    NzTreeSelectComponent.prototype.inputValue;
    /** @type {?} */
    NzTreeSelectComponent.prototype.dropDownPosition;
    /** @type {?} */
    NzTreeSelectComponent.prototype.selectionChangeSubscription;
    /** @type {?} */
    NzTreeSelectComponent.prototype.selectedNodes;
    /** @type {?} */
    NzTreeSelectComponent.prototype.value;
    /** @type {?} */
    NzTreeSelectComponent.prototype.onChange;
    /** @type {?} */
    NzTreeSelectComponent.prototype.onTouched;
    /**
     * @type {?}
     * @private
     */
    NzTreeSelectComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzTreeSelectComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzTreeSelectComponent.prototype.nzTreeService;
    /**
     * @type {?}
     * @private
     */
    NzTreeSelectComponent.prototype.elementRef;
    /** @type {?} */
    NzTreeSelectComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS1zZWxlY3QuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRyZWUtc2VsZWN0L256LXRyZWUtc2VsZWN0LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUNsRCxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsZ0JBQWdCLEVBQWtDLE1BQU0sc0JBQXNCLENBQUM7QUFDN0csT0FBTyxFQUNMLFVBQVUsRUFDVixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osSUFBSSxFQUNKLEtBQUssRUFJTCxRQUFRLEVBQ1IsTUFBTSxFQUNOLFNBQVMsRUFFVCxXQUFXLEVBQ1gsU0FBUyxFQUNWLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBd0IsaUJBQWlCLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUV6RSxPQUFPLEVBQUUsS0FBSyxFQUFFLEVBQUUsSUFBSSxZQUFZLEVBQWdCLE1BQU0sTUFBTSxDQUFDO0FBQy9ELE9BQU8sRUFBRSxNQUFNLEVBQUUsR0FBRyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFN0MsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxVQUFVLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSxnREFBZ0QsQ0FBQztBQUV4RixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFFcEQsT0FBTyxFQUFFLFVBQVUsRUFBcUIsTUFBTSxzQkFBc0IsQ0FBQztBQUNyRSxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFDNUQsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFFL0Q7SUFrSEUs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QUM1QywyQ0FBNEM7O0lBQzVDLDRDQUE2Qzs7SUFDN0MsMkNBQTRDOztJQUM1QyxtREFBb0Q7O0lBQ3BELGtEQUFtQzs7SUFDbkMsd0NBQTZEOztJQUM3RCx1Q0FBd0I7O0lBQ3hCLHVDQUEyQzs7SUFDM0MsOENBQTRCOztJQUM1QixnREFBb0Q7O0lBQ3BELHNEQUE4Qzs7SUFDOUMsOENBQW9HOztJQUNwRyw4Q0FBK0I7O0lBQy9CLG9EQUF1RTs7SUFDdkUsNkNBQThEOztJQUM5RCwwQ0FBd0Q7O0lBQ3hELDBDQUE4RDs7SUFDOUQsK0NBQTBFOztJQUMxRSw0Q0FBdUU7O0lBQ3ZFLHFEQUFnRjs7SUFFaEYsNkNBQW9EOztJQUNwRCx3Q0FBK0M7O0lBQy9DLGlEQUFnRTs7SUFDaEUsb0RBQXlFOztJQUV6RSw2Q0FBcUI7O0lBQ3JCLDRDQUFvQjs7SUFDcEIsMENBQWlCOztJQUNqQiwyQ0FBbUI7O0lBQ25CLDJDQUFnQjs7SUFDaEIsaURBQXlEOztJQUN6RCw0REFBMEM7O0lBQzFDLDhDQUFpQzs7SUFDakMsc0NBQXFCOztJQUVyQix5Q0FBb0Q7O0lBQ3BELDBDQUFtQzs7Ozs7SUFvQ2pDLHlDQUEyQjs7Ozs7SUFDM0Isb0NBQThCOzs7OztJQUM5Qiw4Q0FBMEM7Ozs7O0lBQzFDLDJDQUE4Qjs7SUFDOUIsNENBQStEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQkFDS1NQQUNFIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL2tleWNvZGVzJztcbmltcG9ydCB7IENka0Nvbm5lY3RlZE92ZXJsYXksIENka092ZXJsYXlPcmlnaW4sIENvbm5lY3RlZE92ZXJsYXlQb3NpdGlvbkNoYW5nZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7XG4gIGZvcndhcmRSZWYsXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSG9zdCxcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFJlbmRlcmVyMixcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZFxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBOR19WQUxVRV9BQ0NFU1NPUiB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcblxuaW1wb3J0IHsgbWVyZ2UsIG9mIGFzIG9ic2VydmFibGVPZiwgU3Vic2NyaXB0aW9uIH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyBmaWx0ZXIsIHRhcCB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcblxuaW1wb3J0IHsgc2xpZGVNb3Rpb24gfSBmcm9tICcuLi9jb3JlL2FuaW1hdGlvbi9zbGlkZSc7XG5pbXBvcnQgeyB6b29tTW90aW9uIH0gZnJvbSAnLi4vY29yZS9hbmltYXRpb24vem9vbSc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOelNpemVMRFNUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9zaXplJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56Rm9ybWF0RW1pdEV2ZW50IH0gZnJvbSAnLi4vdHJlZS9pbnRlcmZhY2UnO1xuaW1wb3J0IHsgTnpUcmVlTm9kZSwgTnpUcmVlTm9kZU9wdGlvbnMgfSBmcm9tICcuLi90cmVlL256LXRyZWUtbm9kZSc7XG5pbXBvcnQgeyBOelRyZWVDb21wb25lbnQgfSBmcm9tICcuLi90cmVlL256LXRyZWUuY29tcG9uZW50JztcbmltcG9ydCB7IE56VHJlZVNlbGVjdFNlcnZpY2UgfSBmcm9tICcuL256LXRyZWUtc2VsZWN0LnNlcnZpY2UnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei10cmVlLXNlbGVjdCcsXG4gIGFuaW1hdGlvbnM6IFtzbGlkZU1vdGlvbiwgem9vbU1vdGlvbl0sXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10cmVlLXNlbGVjdC5jb21wb25lbnQuaHRtbCcsXG4gIHByb3ZpZGVyczogW1xuICAgIE56VHJlZVNlbGVjdFNlcnZpY2UsXG4gICAge1xuICAgICAgcHJvdmlkZTogTkdfVkFMVUVfQUNDRVNTT1IsXG4gICAgICB1c2VFeGlzdGluZzogZm9yd2FyZFJlZigoKSA9PiBOelRyZWVTZWxlY3RDb21wb25lbnQpLFxuICAgICAgbXVsdGk6IHRydWVcbiAgICB9XG4gIF0sXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC1zZWxlY3QtbGddJzogJ256U2l6ZT09PVwibGFyZ2VcIicsXG4gICAgJ1tjbGFzcy5hbnQtc2VsZWN0LXNtXSc6ICduelNpemU9PT1cInNtYWxsXCInLFxuICAgICdbY2xhc3MuYW50LXNlbGVjdC1lbmFibGVkXSc6ICchbnpEaXNhYmxlZCcsXG4gICAgJ1tjbGFzcy5hbnQtc2VsZWN0LWRpc2FibGVkXSc6ICduekRpc2FibGVkJyxcbiAgICAnW2NsYXNzLmFudC1zZWxlY3QtYWxsb3ctY2xlYXJdJzogJ256QWxsb3dDbGVhcicsXG4gICAgJ1tjbGFzcy5hbnQtc2VsZWN0LW9wZW5dJzogJ256T3BlbicsXG4gICAgJyhjbGljayknOiAndHJpZ2dlcigpJ1xuICB9LFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICAuYW50LXNlbGVjdC1kcm9wZG93biB7XG4gICAgICAgIHRvcDogMTAwJTtcbiAgICAgICAgbGVmdDogMDtcbiAgICAgICAgcG9zaXRpb246IHJlbGF0aXZlO1xuICAgICAgICB3aWR0aDogMTAwJTtcbiAgICAgICAgbWFyZ2luLXRvcDogNHB4O1xuICAgICAgICBtYXJnaW4tYm90dG9tOiA0cHg7XG4gICAgICAgIG92ZXJmbG93OiBhdXRvO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOelRyZWVTZWxlY3RDb21wb25lbnQgaW1wbGVtZW50cyBDb250cm9sVmFsdWVBY2Nlc3NvciwgT25Jbml0LCBPbkRlc3Ryb3ksIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekFsbG93Q2xlYXIgPSB0cnVlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93RXhwYW5kID0gdHJ1ZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56RHJvcGRvd25NYXRjaFNlbGVjdFdpZHRoID0gdHJ1ZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56Q2hlY2thYmxlID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNob3dTZWFyY2ggPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56RGlzYWJsZWQgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd0xpbmUgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56QXN5bmNEYXRhID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuek11bHRpcGxlID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekRlZmF1bHRFeHBhbmRBbGwgPSBmYWxzZTtcbiAgQElucHV0KCkgbnpOb3RGb3VuZENvbnRlbnQ6IHN0cmluZztcbiAgQElucHV0KCkgbnpOb2RlczogQXJyYXk8TnpUcmVlTm9kZSB8IE56VHJlZU5vZGVPcHRpb25zPiA9IFtdO1xuICBASW5wdXQoKSBuek9wZW4gPSBmYWxzZTtcbiAgQElucHV0KCkgbnpTaXplOiBOelNpemVMRFNUeXBlID0gJ2RlZmF1bHQnO1xuICBASW5wdXQoKSBuelBsYWNlSG9sZGVyID0gJyc7XG4gIEBJbnB1dCgpIG56RHJvcGRvd25TdHlsZTogeyBba2V5OiBzdHJpbmddOiBzdHJpbmcgfTtcbiAgQElucHV0KCkgbnpEZWZhdWx0RXhwYW5kZWRLZXlzOiBzdHJpbmdbXSA9IFtdO1xuICBASW5wdXQoKSBuekRpc3BsYXlXaXRoOiAobm9kZTogTnpUcmVlTm9kZSkgPT4gc3RyaW5nIHwgdW5kZWZpbmVkID0gKG5vZGU6IE56VHJlZU5vZGUpID0+IG5vZGUudGl0bGU7XG4gIEBJbnB1dCgpIG56TWF4VGFnQ291bnQ6IG51bWJlcjtcbiAgQElucHV0KCkgbnpNYXhUYWdQbGFjZWhvbGRlcjogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IE56VHJlZU5vZGVbXSB9PjtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T3BlbkNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Ym9vbGVhbj4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Q2xlYXJlZCA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56UmVtb3ZlZCA9IG5ldyBFdmVudEVtaXR0ZXI8TnpUcmVlTm9kZT4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56RXhwYW5kQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxOekZvcm1hdEVtaXRFdmVudD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56VHJlZUNsaWNrID0gbmV3IEV2ZW50RW1pdHRlcjxOekZvcm1hdEVtaXRFdmVudD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56VHJlZUNoZWNrQm94Q2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxOekZvcm1hdEVtaXRFdmVudD4oKTtcblxuICBAVmlld0NoaWxkKCdpbnB1dEVsZW1lbnQnKSBpbnB1dEVsZW1lbnQ6IEVsZW1lbnRSZWY7XG4gIEBWaWV3Q2hpbGQoJ3RyZWVSZWYnKSB0cmVlUmVmOiBOelRyZWVDb21wb25lbnQ7XG4gIEBWaWV3Q2hpbGQoQ2RrT3ZlcmxheU9yaWdpbikgY2RrT3ZlcmxheU9yaWdpbjogQ2RrT3ZlcmxheU9yaWdpbjtcbiAgQFZpZXdDaGlsZChDZGtDb25uZWN0ZWRPdmVybGF5KSBjZGtDb25uZWN0ZWRPdmVybGF5OiBDZGtDb25uZWN0ZWRPdmVybGF5O1xuXG4gIHRyaWdnZXJXaWR0aDogbnVtYmVyO1xuICBpc0NvbXBvc2luZyA9IGZhbHNlO1xuICBpc0Rlc3Ryb3kgPSB0cnVlO1xuICBpc05vdEZvdW5kID0gZmFsc2U7XG4gIGlucHV0VmFsdWUgPSAnJztcbiAgZHJvcERvd25Qb3NpdGlvbjogJ3RvcCcgfCAnY2VudGVyJyB8ICdib3R0b20nID0gJ2JvdHRvbSc7XG4gIHNlbGVjdGlvbkNoYW5nZVN1YnNjcmlwdGlvbjogU3Vic2NyaXB0aW9uO1xuICBzZWxlY3RlZE5vZGVzOiBOelRyZWVOb2RlW10gPSBbXTtcbiAgdmFsdWU6IHN0cmluZ1tdID0gW107XG5cbiAgb25DaGFuZ2U6ICh2YWx1ZTogc3RyaW5nW10gfCBzdHJpbmcgfCBudWxsKSA9PiB2b2lkO1xuICBvblRvdWNoZWQ6ICgpID0+IHZvaWQgPSAoKSA9PiBudWxsO1xuXG4gIGdldCBwbGFjZUhvbGRlckRpc3BsYXkoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5pbnB1dFZhbHVlIHx8IHRoaXMuaXNDb21wb3NpbmcgfHwgdGhpcy5zZWxlY3RlZE5vZGVzLmxlbmd0aCA/ICdub25lJyA6ICdibG9jayc7XG4gIH1cblxuICBnZXQgc2VhcmNoRGlzcGxheSgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLm56T3BlbiA/ICdibG9jaycgOiAnbm9uZSc7XG4gIH1cblxuICBnZXQgaXNNdWx0aXBsZSgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5uek11bHRpcGxlIHx8IHRoaXMubnpDaGVja2FibGU7XG4gIH1cblxuICBnZXQgc2VsZWN0ZWRWYWx1ZURpc3BsYXkoKTogeyBba2V5OiBzdHJpbmddOiBzdHJpbmcgfSB7XG4gICAgbGV0IHNob3dTZWxlY3RlZFZhbHVlID0gZmFsc2U7XG4gICAgbGV0IG9wYWNpdHkgPSAxO1xuICAgIGlmICghdGhpcy5uelNob3dTZWFyY2gpIHtcbiAgICAgIHNob3dTZWxlY3RlZFZhbHVlID0gdHJ1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHRoaXMubnpPcGVuKSB7XG4gICAgICAgIHNob3dTZWxlY3RlZFZhbHVlID0gISh0aGlzLmlucHV0VmFsdWUgfHwgdGhpcy5pc0NvbXBvc2luZyk7XG4gICAgICAgIGlmIChzaG93U2VsZWN0ZWRWYWx1ZSkge1xuICAgICAgICAgIG9wYWNpdHkgPSAwLjQ7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHNob3dTZWxlY3RlZFZhbHVlID0gdHJ1ZTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIHtcbiAgICAgIGRpc3BsYXk6IHNob3dTZWxlY3RlZFZhbHVlID8gJ2Jsb2NrJyA6ICdub25lJyxcbiAgICAgIG9wYWNpdHk6IGAke29wYWNpdHl9YFxuICAgIH07XG4gIH1cblxuICBjb25zdHJ1Y3RvcihcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByaXZhdGUgbnpUcmVlU2VydmljZTogTnpUcmVlU2VsZWN0U2VydmljZSxcbiAgICBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgQEhvc3QoKSBAT3B0aW9uYWwoKSBwdWJsaWMgbm9BbmltYXRpb24/OiBOek5vQW5pbWF0aW9uRGlyZWN0aXZlXG4gICkge1xuICAgIHRoaXMucmVuZGVyZXIuYWRkQ2xhc3ModGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtc2VsZWN0Jyk7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmlzRGVzdHJveSA9IGZhbHNlO1xuICAgIHRoaXMuc2VsZWN0aW9uQ2hhbmdlU3Vic2NyaXB0aW9uID0gdGhpcy5zdWJzY3JpYmVTZWxlY3Rpb25DaGFuZ2UoKTtcbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuaXNEZXN0cm95ID0gdHJ1ZTtcbiAgICB0aGlzLmNsb3NlRHJvcERvd24oKTtcbiAgICB0aGlzLnNlbGVjdGlvbkNoYW5nZVN1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpO1xuICB9XG5cbiAgc2V0RGlzYWJsZWRTdGF0ZShpc0Rpc2FibGVkOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5uekRpc2FibGVkID0gaXNEaXNhYmxlZDtcbiAgICB0aGlzLmNsb3NlRHJvcERvd24oKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5oYXNPd25Qcm9wZXJ0eSgnbnpOb2RlcycpKSB7XG4gICAgICB0aGlzLnVwZGF0ZVNlbGVjdGVkTm9kZXModHJ1ZSk7XG4gICAgfVxuICB9XG5cbiAgd3JpdGVWYWx1ZSh2YWx1ZTogc3RyaW5nW10gfCBzdHJpbmcpOiB2b2lkIHtcbiAgICBpZiAodmFsdWUpIHtcbiAgICAgIGlmICh0aGlzLmlzTXVsdGlwbGUgJiYgQXJyYXkuaXNBcnJheSh2YWx1ZSkpIHtcbiAgICAgICAgdGhpcy52YWx1ZSA9IHZhbHVlO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy52YWx1ZSA9IFt2YWx1ZSBhcyBzdHJpbmddO1xuICAgICAgfVxuICAgICAgdGhpcy51cGRhdGVTZWxlY3RlZE5vZGVzKHRydWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnZhbHVlID0gW107XG4gICAgICB0aGlzLnNlbGVjdGVkTm9kZXMuZm9yRWFjaChub2RlID0+IHtcbiAgICAgICAgdGhpcy5yZW1vdmVTZWxlY3RlZChub2RlLCBmYWxzZSk7XG4gICAgICB9KTtcbiAgICAgIHRoaXMuc2VsZWN0ZWROb2RlcyA9IFtdO1xuICAgIH1cbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46IChfOiBzdHJpbmdbXSB8IHN0cmluZyB8IG51bGwpID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLm9uQ2hhbmdlID0gZm47XG4gIH1cblxuICByZWdpc3Rlck9uVG91Y2hlZChmbjogKCkgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25Ub3VjaGVkID0gZm47XG4gIH1cblxuICB0cmlnZ2VyKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56RGlzYWJsZWQgfHwgKCF0aGlzLm56RGlzYWJsZWQgJiYgdGhpcy5uek9wZW4pKSB7XG4gICAgICB0aGlzLmNsb3NlRHJvcERvd24oKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5vcGVuRHJvcGRvd24oKTtcbiAgICAgIGlmICh0aGlzLm56U2hvd1NlYXJjaCB8fCB0aGlzLmlzTXVsdGlwbGUpIHtcbiAgICAgICAgdGhpcy5mb2N1c09uSW5wdXQoKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBvcGVuRHJvcGRvd24oKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLm56RGlzYWJsZWQpIHtcbiAgICAgIHRoaXMubnpPcGVuID0gdHJ1ZTtcbiAgICAgIHRoaXMubnpPcGVuQ2hhbmdlLmVtaXQodGhpcy5uek9wZW4pO1xuICAgICAgdGhpcy51cGRhdGVDZGtDb25uZWN0ZWRPdmVybGF5U3RhdHVzKCk7XG4gICAgICB0aGlzLnVwZGF0ZVBvc2l0aW9uKCk7XG4gICAgfVxuICB9XG5cbiAgY2xvc2VEcm9wRG93bigpOiB2b2lkIHtcbiAgICB0aGlzLm9uVG91Y2hlZCgpO1xuICAgIHRoaXMubnpPcGVuID0gZmFsc2U7XG4gICAgdGhpcy5uek9wZW5DaGFuZ2UuZW1pdCh0aGlzLm56T3Blbik7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICBvbktleURvd25JbnB1dChlOiBLZXlib2FyZEV2ZW50KTogdm9pZCB7XG4gICAgY29uc3Qga2V5Q29kZSA9IGUua2V5Q29kZTtcbiAgICBjb25zdCBldmVudFRhcmdldCA9IGUudGFyZ2V0IGFzIEhUTUxJbnB1dEVsZW1lbnQ7XG4gICAgaWYgKHRoaXMuaXNNdWx0aXBsZSAmJiAhZXZlbnRUYXJnZXQudmFsdWUgJiYga2V5Q29kZSA9PT0gQkFDS1NQQUNFKSB7XG4gICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICBpZiAodGhpcy5zZWxlY3RlZE5vZGVzLmxlbmd0aCkge1xuICAgICAgICBjb25zdCByZW1vdmVOb2RlID0gdGhpcy5zZWxlY3RlZE5vZGVzW3RoaXMuc2VsZWN0ZWROb2Rlcy5sZW5ndGggLSAxXTtcbiAgICAgICAgdGhpcy5yZW1vdmVTZWxlY3RlZChyZW1vdmVOb2RlKTtcbiAgICAgICAgdGhpcy5uelRyZWVTZXJ2aWNlIS50cmlnZ2VyRXZlbnRDaGFuZ2UkIS5uZXh0KHtcbiAgICAgICAgICBldmVudE5hbWU6ICdyZW1vdmVTZWxlY3QnLFxuICAgICAgICAgIG5vZGU6IHJlbW92ZU5vZGVcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgb25FeHBhbmRlZEtleXNDaGFuZ2UodmFsdWU6IE56Rm9ybWF0RW1pdEV2ZW50KTogdm9pZCB7XG4gICAgdGhpcy5uekV4cGFuZENoYW5nZS5lbWl0KHZhbHVlKTtcbiAgICB0aGlzLm56RGVmYXVsdEV4cGFuZGVkS2V5cyA9IFsuLi52YWx1ZS5rZXlzIV07XG4gIH1cblxuICBzZXRJbnB1dFZhbHVlKHZhbHVlOiBzdHJpbmcpOiB2b2lkIHtcbiAgICB0aGlzLmlucHV0VmFsdWUgPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZUlucHV0V2lkdGgoKTtcbiAgICB0aGlzLnVwZGF0ZVBvc2l0aW9uKCk7XG4gIH1cblxuICByZW1vdmVTZWxlY3RlZChub2RlOiBOelRyZWVOb2RlLCBlbWl0OiBib29sZWFuID0gdHJ1ZSwgZXZlbnQ/OiBNb3VzZUV2ZW50KTogdm9pZCB7XG4gICAgbm9kZS5pc1NlbGVjdGVkID0gZmFsc2U7XG4gICAgbm9kZS5pc0NoZWNrZWQgPSBmYWxzZTtcbiAgICBpZiAodGhpcy5uekNoZWNrYWJsZSkge1xuICAgICAgdGhpcy5uelRyZWVTZXJ2aWNlLmNvbmR1Y3Qobm9kZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMubnpUcmVlU2VydmljZS5zZXRTZWxlY3RlZE5vZGVMaXN0KG5vZGUsIHRoaXMubnpNdWx0aXBsZSk7XG4gICAgfVxuXG4gICAgaWYgKGVtaXQpIHtcbiAgICAgIHRoaXMubnpSZW1vdmVkLmVtaXQobm9kZSk7XG4gICAgfVxuXG4gICAgLy8gRG8gbm90IHRyaWdnZXIgdGhlIHBvcHVwXG4gICAgaWYgKGV2ZW50ICYmIGV2ZW50LnN0b3BQcm9wYWdhdGlvbikge1xuICAgICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgfVxuICB9XG5cbiAgZm9jdXNPbklucHV0KCk6IHZvaWQge1xuICAgIHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgaWYgKHRoaXMuaW5wdXRFbGVtZW50KSB7XG4gICAgICAgIHRoaXMuaW5wdXRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQuZm9jdXMoKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIHN1YnNjcmliZVNlbGVjdGlvbkNoYW5nZSgpOiBTdWJzY3JpcHRpb24ge1xuICAgIHJldHVybiBtZXJnZShcbiAgICAgIHRoaXMubnpUcmVlQ2xpY2sucGlwZShcbiAgICAgICAgdGFwKChldmVudDogTnpGb3JtYXRFbWl0RXZlbnQpID0+IHtcbiAgICAgICAgICBjb25zdCBub2RlID0gZXZlbnQubm9kZSE7XG4gICAgICAgICAgaWYgKHRoaXMubnpDaGVja2FibGUgJiYgIW5vZGUuaXNEaXNhYmxlZCAmJiAhbm9kZS5pc0Rpc2FibGVDaGVja2JveCkge1xuICAgICAgICAgICAgbm9kZS5pc0NoZWNrZWQgPSAhbm9kZS5pc0NoZWNrZWQ7XG4gICAgICAgICAgICB0aGlzLm56VHJlZVNlcnZpY2UuY29uZHVjdChub2RlKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKHRoaXMubnpDaGVja2FibGUpIHtcbiAgICAgICAgICAgIG5vZGUuaXNTZWxlY3RlZCA9IGZhbHNlO1xuICAgICAgICAgIH1cbiAgICAgICAgfSksXG4gICAgICAgIGZpbHRlcigoZXZlbnQ6IE56Rm9ybWF0RW1pdEV2ZW50KSA9PiB7XG4gICAgICAgICAgY29uc3Qgbm9kZSA9IGV2ZW50Lm5vZGUhO1xuICAgICAgICAgIHJldHVybiB0aGlzLm56Q2hlY2thYmxlID8gIW5vZGUuaXNEaXNhYmxlZCAmJiAhbm9kZS5pc0Rpc2FibGVDaGVja2JveCA6ICFub2RlLmlzRGlzYWJsZWQ7XG4gICAgICAgIH0pXG4gICAgICApLFxuICAgICAgdGhpcy5uekNoZWNrYWJsZSA/IHRoaXMubnpUcmVlQ2hlY2tCb3hDaGFuZ2UgOiBvYnNlcnZhYmxlT2YoKSxcbiAgICAgIHRoaXMubnpDbGVhcmVkLFxuICAgICAgdGhpcy5uelJlbW92ZWRcbiAgICApLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICB0aGlzLnVwZGF0ZVNlbGVjdGVkTm9kZXMoKTtcbiAgICAgIGNvbnN0IHZhbHVlID0gdGhpcy5zZWxlY3RlZE5vZGVzLm1hcChub2RlID0+IG5vZGUua2V5ISk7XG4gICAgICB0aGlzLnZhbHVlID0gWy4uLnZhbHVlXTtcbiAgICAgIGlmICh0aGlzLm56U2hvd1NlYXJjaCB8fCB0aGlzLmlzTXVsdGlwbGUpIHtcbiAgICAgICAgdGhpcy5pbnB1dFZhbHVlID0gJyc7XG4gICAgICAgIHRoaXMuaXNOb3RGb3VuZCA9IGZhbHNlO1xuICAgICAgfVxuICAgICAgaWYgKHRoaXMuaXNNdWx0aXBsZSkge1xuICAgICAgICB0aGlzLm9uQ2hhbmdlKHZhbHVlKTtcbiAgICAgICAgdGhpcy5mb2N1c09uSW5wdXQoKTtcbiAgICAgICAgdGhpcy51cGRhdGVQb3NpdGlvbigpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5jbG9zZURyb3BEb3duKCk7XG4gICAgICAgIHRoaXMub25DaGFuZ2UodmFsdWUubGVuZ3RoID8gdmFsdWVbMF0gOiBudWxsKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIHVwZGF0ZVNlbGVjdGVkTm9kZXMoaW5pdDogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgaWYgKGluaXQpIHtcbiAgICAgIGxldCBub2RlcztcbiAgICAgIHRoaXMubnpUcmVlU2VydmljZS5pc011bHRpcGxlID0gdGhpcy5pc011bHRpcGxlO1xuICAgICAgaWYgKCF0aGlzLm56VHJlZVNlcnZpY2UuaXNBcnJheU9mTnpUcmVlTm9kZSh0aGlzLm56Tm9kZXMpKSB7XG4gICAgICAgIC8vIGhhcyBub3QgYmVlbiBuZXcgTnpUcmVlTm9kZVxuICAgICAgICBub2RlcyA9IHRoaXMubnpOb2Rlcy5tYXAoaXRlbSA9PiBuZXcgTnpUcmVlTm9kZShpdGVtLCB1bmRlZmluZWQsIHRoaXMubnpUcmVlU2VydmljZSkpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbm9kZXMgPSB0aGlzLm56Tm9kZXMubWFwKGl0ZW0gPT4gbmV3IE56VHJlZU5vZGUoeyAuLi5pdGVtLm9yaWdpbiB9LCB1bmRlZmluZWQsIHRoaXMubnpUcmVlU2VydmljZSkpO1xuICAgICAgfVxuICAgICAgdGhpcy5uelRyZWVTZXJ2aWNlLmluaXRUcmVlKG5vZGVzKTtcbiAgICAgIGlmICh0aGlzLm56Q2hlY2thYmxlKSB7XG4gICAgICAgIHRoaXMubnpUcmVlU2VydmljZS5jYWxjQ2hlY2tlZEtleXModGhpcy52YWx1ZSwgbm9kZXMpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5uelRyZWVTZXJ2aWNlLmNhbGNTZWxlY3RlZEtleXModGhpcy52YWx1ZSwgbm9kZXMsIHRoaXMuaXNNdWx0aXBsZSk7XG4gICAgICB9XG4gICAgfVxuICAgIHRoaXMuc2VsZWN0ZWROb2RlcyA9IFtcbiAgICAgIC4uLih0aGlzLm56Q2hlY2thYmxlID8gdGhpcy5uelRyZWVTZXJ2aWNlLmdldENoZWNrZWROb2RlTGlzdCgpIDogdGhpcy5uelRyZWVTZXJ2aWNlLmdldFNlbGVjdGVkTm9kZUxpc3QoKSlcbiAgICBdO1xuICB9XG5cbiAgdXBkYXRlUG9zaXRpb24oKTogdm9pZCB7XG4gICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICBpZiAodGhpcy5jZGtDb25uZWN0ZWRPdmVybGF5ICYmIHRoaXMuY2RrQ29ubmVjdGVkT3ZlcmxheS5vdmVybGF5UmVmKSB7XG4gICAgICAgIHRoaXMuY2RrQ29ubmVjdGVkT3ZlcmxheS5vdmVybGF5UmVmLnVwZGF0ZVBvc2l0aW9uKCk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICBvblBvc2l0aW9uQ2hhbmdlKHBvc2l0aW9uOiBDb25uZWN0ZWRPdmVybGF5UG9zaXRpb25DaGFuZ2UpOiB2b2lkIHtcbiAgICB0aGlzLmRyb3BEb3duUG9zaXRpb24gPSBwb3NpdGlvbi5jb25uZWN0aW9uUGFpci5vcmlnaW5ZO1xuICB9XG5cbiAgdXBkYXRlSW5wdXRXaWR0aCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc011bHRpcGxlICYmIHRoaXMuaW5wdXRFbGVtZW50KSB7XG4gICAgICBpZiAodGhpcy5pbnB1dFZhbHVlIHx8IHRoaXMuaXNDb21wb3NpbmcpIHtcbiAgICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZShcbiAgICAgICAgICB0aGlzLmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LFxuICAgICAgICAgICd3aWR0aCcsXG4gICAgICAgICAgYCR7dGhpcy5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudC5zY3JvbGxXaWR0aH1weGBcbiAgICAgICAgKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlU3R5bGUodGhpcy5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudCwgJ3dpZHRoJyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgb25DbGVhclNlbGVjdGlvbigkZXZlbnQ6IE1vdXNlRXZlbnQpOiB2b2lkIHtcbiAgICAkZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgJGV2ZW50LnByZXZlbnREZWZhdWx0KCk7XG4gICAgdGhpcy5zZWxlY3RlZE5vZGVzLmZvckVhY2gobm9kZSA9PiB7XG4gICAgICB0aGlzLnJlbW92ZVNlbGVjdGVkKG5vZGUsIGZhbHNlKTtcbiAgICB9KTtcbiAgICB0aGlzLm56Q2xlYXJlZC5lbWl0KCk7XG4gIH1cblxuICBzZXRTZWFyY2hWYWx1ZXMoJGV2ZW50OiBOekZvcm1hdEVtaXRFdmVudCk6IHZvaWQge1xuICAgIFByb21pc2UucmVzb2x2ZSgpLnRoZW4oKCkgPT4ge1xuICAgICAgdGhpcy5pc05vdEZvdW5kID0gKHRoaXMubnpTaG93U2VhcmNoIHx8IHRoaXMuaXNNdWx0aXBsZSkgJiYgISF0aGlzLmlucHV0VmFsdWUgJiYgJGV2ZW50Lm1hdGNoZWRLZXlzIS5sZW5ndGggPT09IDA7XG4gICAgfSk7XG4gIH1cblxuICB1cGRhdGVDZGtDb25uZWN0ZWRPdmVybGF5U3RhdHVzKCk6IHZvaWQge1xuICAgIHRoaXMudHJpZ2dlcldpZHRoID0gdGhpcy5jZGtPdmVybGF5T3JpZ2luLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKS53aWR0aDtcbiAgfVxuXG4gIHRyYWNrVmFsdWUoX2luZGV4OiBudW1iZXIsIG9wdGlvbjogTnpUcmVlTm9kZSk6IHN0cmluZyB7XG4gICAgcmV0dXJuIG9wdGlvbi5rZXkhO1xuICB9XG59XG4iXX0=