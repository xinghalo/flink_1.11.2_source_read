/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { BACKSPACE, DOWN_ARROW, ENTER, ESCAPE, LEFT_ARROW, RIGHT_ARROW, UP_ARROW } from '@angular/cdk/keycodes';
import { CdkConnectedOverlay } from '@angular/cdk/overlay';
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Host, HostListener, Input, Optional, Output, QueryList, Renderer2, TemplateRef, ViewChild, ViewChildren, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { slideMotion } from '../core/animation/slide';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { DEFAULT_CASCADER_POSITIONS } from '../core/overlay/overlay-position';
import { toArray } from '../core/util/array';
import { InputBoolean } from '../core/util/convert';
import { NzCascaderOptionComponent } from './nz-cascader-li.component';
import { NzCascaderService } from './nz-cascader.service';
/** @type {?} */
var defaultDisplayRender = (/**
 * @param {?} labels
 * @return {?}
 */
function (labels) { return labels.join(' / '); });
var ɵ0 = defaultDisplayRender;
var NzCascaderComponent = /** @class */ (function () {
    function NzCascaderComponent(cascaderService, cdr, elementRef, renderer, noAnimation) {
        this.cascaderService = cascaderService;
        this.cdr = cdr;
        this.noAnimation = noAnimation;
        this.nzShowInput = true;
        this.nzShowArrow = true;
        this.nzAllowClear = true;
        this.nzAutoFocus = false;
        this.nzChangeOnSelect = false;
        this.nzDisabled = false;
        this.nzExpandTrigger = 'click';
        this.nzValueProperty = 'value';
        this.nzLabelProperty = 'label';
        this.nzSize = 'default';
        this.nzPlaceHolder = 'Please select'; // TODO: i18n?
        this.nzMouseEnterDelay = 150; // ms
        // ms
        this.nzMouseLeaveDelay = 150; // ms
        // ms
        this.nzTriggerAction = (/** @type {?} */ (['click']));
        this.nzSelectionChange = new EventEmitter();
        this.nzSelect = new EventEmitter();
        this.nzClear = new EventEmitter();
        this.nzVisibleChange = new EventEmitter(); // Not exposed, only for test
        // Not exposed, only for test
        this.nzChange = new EventEmitter(); // Not exposed, only for test
        this.dropDownPosition = 'bottom';
        this.menuVisible = false;
        this.isLoading = false;
        this.labelRenderContext = {};
        this.onChange = Function.prototype;
        this.onTouched = Function.prototype;
        this.positions = tslib_1.__spread(DEFAULT_CASCADER_POSITIONS);
        this.isFocused = false;
        this.$destroy = new Subject();
        this.inputString = '';
        this.isOpening = false;
        this.el = elementRef.nativeElement;
        this.cascaderService.withComponent(this);
        renderer.addClass(elementRef.nativeElement, 'ant-cascader');
        renderer.addClass(elementRef.nativeElement, 'ant-cascader-picker');
    }
    Object.defineProperty(NzCascaderComponent.prototype, "nzOptions", {
        get: 
        // tslint:disable-line:no-any
        /**
         * @return {?}
         */
        function () {
            return this.cascaderService.nzOptions;
        },
        set: /**
         * @param {?} options
         * @return {?}
         */
        function (options) {
            this.cascaderService.withOptions(options);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "inSearchingMode", {
        get: /**
         * @return {?}
         */
        function () {
            return this.cascaderService.inSearchingMode;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "inputValue", {
        get: /**
         * @return {?}
         */
        function () {
            return this.inputString;
        },
        set: /**
         * @param {?} inputValue
         * @return {?}
         */
        function (inputValue) {
            this.inputString = inputValue;
            this.toggleSearchingMode(!!inputValue);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "menuCls", {
        get: /**
         * @return {?}
         */
        function () {
            var _a;
            return _a = {}, _a["" + this.nzMenuClassName] = !!this.nzMenuClassName, _a;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "menuColumnCls", {
        get: /**
         * @return {?}
         */
        function () {
            var _a;
            return _a = {}, _a["" + this.nzColumnClassName] = !!this.nzColumnClassName, _a;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "hasInput", {
        get: /**
         * @private
         * @return {?}
         */
        function () {
            return !!this.inputValue;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "hasValue", {
        get: /**
         * @private
         * @return {?}
         */
        function () {
            return this.cascaderService.values && this.cascaderService.values.length > 0;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "showPlaceholder", {
        get: /**
         * @return {?}
         */
        function () {
            return !(this.hasInput || this.hasValue);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "clearIconVisible", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzAllowClear && !this.nzDisabled && (this.hasValue || this.hasInput);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCascaderComponent.prototype, "isLabelRenderTemplate", {
        get: /**
         * @return {?}
         */
        function () {
            return !!this.nzLabelRender;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var srv = this.cascaderService;
        srv.$redraw.pipe(takeUntil(this.$destroy)).subscribe((/**
         * @return {?}
         */
        function () {
            // These operations would not mutate data.
            _this.checkChildren();
            _this.buildDisplayLabel();
            _this.reposition();
            _this.cdr.markForCheck();
        }));
        srv.$loading.pipe(takeUntil(this.$destroy)).subscribe((/**
         * @param {?} loading
         * @return {?}
         */
        function (loading) {
            _this.isLoading = loading;
        }));
        srv.$optionSelected.pipe(takeUntil(this.$destroy)).subscribe((/**
         * @param {?} data
         * @return {?}
         */
        function (data) {
            if (!data) {
                _this.onChange([]);
                _this.nzSelect.emit(null);
            }
            else {
                var option = data.option, index = data.index;
                /** @type {?} */
                var shouldClose = option.isLeaf;
                if (shouldClose) {
                    _this.delaySetMenuVisible(false);
                }
                _this.onChange(_this.cascaderService.values);
                _this.nzSelectionChange.emit(_this.cascaderService.selectedOptions);
                _this.nzSelect.emit({ option: option, index: index });
                _this.cdr.markForCheck();
            }
        }));
        srv.$quitSearching.pipe(takeUntil(this.$destroy)).subscribe((/**
         * @return {?}
         */
        function () {
            _this.inputString = '';
            _this.dropdownWidthStyle = '';
        }));
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.clearDelayMenuTimer();
        this.clearDelaySelectTimer();
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzCascaderComponent.prototype.registerOnChange = /**
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
    NzCascaderComponent.prototype.registerOnTouched = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this.onTouched = fn;
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {?} value
     * @return {?}
     */
    NzCascaderComponent.prototype.writeValue = 
    // tslint:disable-next-line:no-any
    /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.cascaderService.values = toArray(value);
        this.cascaderService.syncOptions(true);
    };
    /**
     * @param {?} visible
     * @param {?=} delay
     * @param {?=} setOpening
     * @return {?}
     */
    NzCascaderComponent.prototype.delaySetMenuVisible = /**
     * @param {?} visible
     * @param {?=} delay
     * @param {?=} setOpening
     * @return {?}
     */
    function (visible, delay, setOpening) {
        var _this = this;
        if (delay === void 0) { delay = 100; }
        if (setOpening === void 0) { setOpening = false; }
        this.clearDelayMenuTimer();
        if (delay) {
            if (visible && setOpening) {
                this.isOpening = true;
            }
            this.delayMenuTimer = setTimeout((/**
             * @return {?}
             */
            function () {
                _this.setMenuVisible(visible);
                _this.cdr.detectChanges();
                _this.clearDelayMenuTimer();
                if (visible) {
                    setTimeout((/**
                     * @return {?}
                     */
                    function () {
                        _this.isOpening = false;
                    }), 100);
                }
            }), delay);
        }
        else {
            this.setMenuVisible(visible);
        }
    };
    /**
     * @param {?} visible
     * @return {?}
     */
    NzCascaderComponent.prototype.setMenuVisible = /**
     * @param {?} visible
     * @return {?}
     */
    function (visible) {
        if (this.nzDisabled || this.menuVisible === visible) {
            return;
        }
        if (visible) {
            this.cascaderService.syncOptions();
        }
        this.menuVisible = visible;
        this.nzVisibleChange.emit(visible);
        this.cdr.detectChanges();
    };
    /**
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.clearDelayMenuTimer = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.delayMenuTimer) {
            clearTimeout(this.delayMenuTimer);
            this.delayMenuTimer = null;
        }
    };
    /**
     * @param {?=} event
     * @return {?}
     */
    NzCascaderComponent.prototype.clearSelection = /**
     * @param {?=} event
     * @return {?}
     */
    function (event) {
        if (event) {
            event.preventDefault();
            event.stopPropagation();
        }
        this.labelRenderText = '';
        this.labelRenderContext = {};
        this.inputValue = '';
        this.setMenuVisible(false);
        this.cascaderService.clear();
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.getSubmitValue = 
    // tslint:disable-next-line:no-any
    /**
     * @return {?}
     */
    function () {
        var _this = this;
        return this.cascaderService.selectedOptions.map((/**
         * @param {?} o
         * @return {?}
         */
        function (o) { return _this.cascaderService.getOptionValue(o); }));
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.focus = /**
     * @return {?}
     */
    function () {
        if (!this.isFocused) {
            (this.input ? this.input.nativeElement : this.el).focus();
            this.isFocused = true;
        }
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.blur = /**
     * @return {?}
     */
    function () {
        if (this.isFocused) {
            (this.input ? this.input.nativeElement : this.el).blur();
            this.isFocused = false;
        }
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.handleInputBlur = /**
     * @return {?}
     */
    function () {
        this.menuVisible ? this.focus() : this.blur();
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.handleInputFocus = /**
     * @return {?}
     */
    function () {
        this.focus();
    };
    /**
     * @param {?} event
     * @return {?}
     */
    NzCascaderComponent.prototype.onKeyDown = /**
     * @param {?} event
     * @return {?}
     */
    function (event) {
        /** @type {?} */
        var keyCode = event.keyCode;
        if (keyCode !== DOWN_ARROW &&
            keyCode !== UP_ARROW &&
            keyCode !== LEFT_ARROW &&
            keyCode !== RIGHT_ARROW &&
            keyCode !== ENTER &&
            keyCode !== BACKSPACE &&
            keyCode !== ESCAPE) {
            return;
        }
        // Press any keys above to reopen menu.
        if (!this.menuVisible && keyCode !== BACKSPACE && keyCode !== ESCAPE) {
            return this.setMenuVisible(true);
        }
        // Make these keys work as default in searching mode.
        if (this.inSearchingMode && (keyCode === BACKSPACE || keyCode === LEFT_ARROW || keyCode === RIGHT_ARROW)) {
            return;
        }
        // Interact with the component.
        if (this.menuVisible) {
            event.preventDefault();
            if (keyCode === DOWN_ARROW) {
                this.moveUpOrDown(false);
            }
            else if (keyCode === UP_ARROW) {
                this.moveUpOrDown(true);
            }
            else if (keyCode === LEFT_ARROW) {
                this.moveLeft();
            }
            else if (keyCode === RIGHT_ARROW) {
                this.moveRight();
            }
            else if (keyCode === ENTER) {
                this.onEnter();
            }
        }
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.onTriggerClick = /**
     * @return {?}
     */
    function () {
        if (this.nzDisabled) {
            return;
        }
        if (this.nzShowSearch) {
            this.focus();
        }
        if (this.isActionTrigger('click')) {
            this.delaySetMenuVisible(!this.menuVisible, 100);
        }
        this.onTouched();
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.onTriggerMouseEnter = /**
     * @return {?}
     */
    function () {
        if (this.nzDisabled) {
            return;
        }
        if (this.isActionTrigger('hover')) {
            this.delaySetMenuVisible(true, this.nzMouseEnterDelay, true);
        }
    };
    /**
     * @param {?} event
     * @return {?}
     */
    NzCascaderComponent.prototype.onTriggerMouseLeave = /**
     * @param {?} event
     * @return {?}
     */
    function (event) {
        if (this.nzDisabled) {
            return;
        }
        if (!this.menuVisible || this.isOpening) {
            event.preventDefault();
            return;
        }
        if (this.isActionTrigger('hover')) {
            /** @type {?} */
            var mouseTarget = (/** @type {?} */ (event.relatedTarget));
            /** @type {?} */
            var hostEl = this.el;
            /** @type {?} */
            var menuEl = this.menu && ((/** @type {?} */ (this.menu.nativeElement)));
            if (hostEl.contains(mouseTarget) || (menuEl && menuEl.contains(mouseTarget))) {
                return;
            }
            this.delaySetMenuVisible(false, this.nzMouseLeaveDelay);
        }
    };
    /**
     * @private
     * @param {?} action
     * @return {?}
     */
    NzCascaderComponent.prototype.isActionTrigger = /**
     * @private
     * @param {?} action
     * @return {?}
     */
    function (action) {
        return typeof this.nzTriggerAction === 'string'
            ? this.nzTriggerAction === action
            : this.nzTriggerAction.indexOf(action) !== -1;
    };
    /**
     * @param {?} option
     * @param {?} columnIndex
     * @param {?} event
     * @return {?}
     */
    NzCascaderComponent.prototype.onOptionClick = /**
     * @param {?} option
     * @param {?} columnIndex
     * @param {?} event
     * @return {?}
     */
    function (option, columnIndex, event) {
        if (event) {
            event.preventDefault();
        }
        if (option && option.disabled) {
            return;
        }
        this.el.focus();
        this.inSearchingMode
            ? this.cascaderService.setSearchOptionSelected((/** @type {?} */ (option)))
            : this.cascaderService.setOptionActivated(option, columnIndex, true);
    };
    /**
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.onEnter = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var columnIndex = Math.max(this.cascaderService.activatedOptions.length - 1, 0);
        /** @type {?} */
        var option = this.cascaderService.activatedOptions[columnIndex];
        if (option && !option.disabled) {
            this.inSearchingMode
                ? this.cascaderService.setSearchOptionSelected((/** @type {?} */ (option)))
                : this.cascaderService.setOptionActivated(option, columnIndex, true);
        }
    };
    /**
     * @private
     * @param {?} isUp
     * @return {?}
     */
    NzCascaderComponent.prototype.moveUpOrDown = /**
     * @private
     * @param {?} isUp
     * @return {?}
     */
    function (isUp) {
        /** @type {?} */
        var columnIndex = Math.max(this.cascaderService.activatedOptions.length - 1, 0);
        /** @type {?} */
        var activeOption = this.cascaderService.activatedOptions[columnIndex];
        /** @type {?} */
        var options = this.cascaderService.columns[columnIndex] || [];
        /** @type {?} */
        var length = options.length;
        /** @type {?} */
        var nextIndex = -1;
        if (!activeOption) {
            // Not selected options in this column
            nextIndex = isUp ? length : -1;
        }
        else {
            nextIndex = options.indexOf(activeOption);
        }
        while (true) {
            nextIndex = isUp ? nextIndex - 1 : nextIndex + 1;
            if (nextIndex < 0 || nextIndex >= length) {
                break;
            }
            /** @type {?} */
            var nextOption = options[nextIndex];
            if (!nextOption || nextOption.disabled) {
                continue;
            }
            this.cascaderService.setOptionActivated(nextOption, columnIndex);
            break;
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.moveLeft = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var options = this.cascaderService.activatedOptions;
        if (options.length) {
            options.pop(); // Remove the last one
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.moveRight = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var length = this.cascaderService.activatedOptions.length;
        /** @type {?} */
        var options = this.cascaderService.columns[length];
        if (options && options.length) {
            /** @type {?} */
            var nextOpt = options.find((/**
             * @param {?} o
             * @return {?}
             */
            function (o) { return !o.disabled; }));
            if (nextOpt) {
                this.cascaderService.setOptionActivated(nextOpt, length);
            }
        }
    };
    /**
     * @param {?} option
     * @param {?} columnIndex
     * @param {?} event
     * @return {?}
     */
    NzCascaderComponent.prototype.onOptionMouseEnter = /**
     * @param {?} option
     * @param {?} columnIndex
     * @param {?} event
     * @return {?}
     */
    function (option, columnIndex, event) {
        event.preventDefault();
        if (this.nzExpandTrigger === 'hover' && !option.isLeaf) {
            this.delaySelectOption(option, columnIndex, true);
        }
    };
    /**
     * @param {?} option
     * @param {?} columnIndex
     * @param {?} event
     * @return {?}
     */
    NzCascaderComponent.prototype.onOptionMouseLeave = /**
     * @param {?} option
     * @param {?} columnIndex
     * @param {?} event
     * @return {?}
     */
    function (option, columnIndex, event) {
        event.preventDefault();
        if (this.nzExpandTrigger === 'hover' && !option.isLeaf) {
            this.delaySelectOption(option, columnIndex, false);
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.clearDelaySelectTimer = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.delaySelectTimer) {
            clearTimeout(this.delaySelectTimer);
            this.delaySelectTimer = null;
        }
    };
    /**
     * @private
     * @param {?} option
     * @param {?} index
     * @param {?} doSelect
     * @return {?}
     */
    NzCascaderComponent.prototype.delaySelectOption = /**
     * @private
     * @param {?} option
     * @param {?} index
     * @param {?} doSelect
     * @return {?}
     */
    function (option, index, doSelect) {
        var _this = this;
        this.clearDelaySelectTimer();
        if (doSelect) {
            this.delaySelectTimer = setTimeout((/**
             * @return {?}
             */
            function () {
                _this.cascaderService.setOptionActivated(option, index);
                _this.delaySelectTimer = null;
            }), 150);
        }
    };
    /**
     * @private
     * @param {?} toSearching
     * @return {?}
     */
    NzCascaderComponent.prototype.toggleSearchingMode = /**
     * @private
     * @param {?} toSearching
     * @return {?}
     */
    function (toSearching) {
        if (this.inSearchingMode !== toSearching) {
            this.cascaderService.toggleSearchingMode(toSearching);
            this.dropdownWidthStyle = toSearching ? this.input.nativeElement.offsetWidth + "px" : '';
        }
        if (this.inSearchingMode) {
            this.cascaderService.prepareSearchOptions(this.inputValue);
        }
    };
    /**
     * @param {?} option
     * @param {?} index
     * @return {?}
     */
    NzCascaderComponent.prototype.isOptionActivated = /**
     * @param {?} option
     * @param {?} index
     * @return {?}
     */
    function (option, index) {
        /** @type {?} */
        var activeOpt = this.cascaderService.activatedOptions[index];
        return activeOpt === option;
    };
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    NzCascaderComponent.prototype.setDisabledState = /**
     * @param {?} isDisabled
     * @return {?}
     */
    function (isDisabled) {
        if (isDisabled) {
            this.closeMenu();
        }
        this.nzDisabled = isDisabled;
    };
    /**
     * @return {?}
     */
    NzCascaderComponent.prototype.closeMenu = /**
     * @return {?}
     */
    function () {
        this.blur();
        this.clearDelayMenuTimer();
        this.setMenuVisible(false);
    };
    /**
     * @param {?} position
     * @return {?}
     */
    NzCascaderComponent.prototype.onPositionChange = /**
     * @param {?} position
     * @return {?}
     */
    function (position) {
        /** @type {?} */
        var newValue = position.connectionPair.originY === 'bottom' ? 'bottom' : 'top';
        if (this.dropDownPosition !== newValue) {
            this.dropDownPosition = newValue;
            this.cdr.detectChanges();
        }
    };
    /**
     * Reposition the cascader panel. When a menu opens, the cascader expands
     * and may exceed the boundary of browser's window.
     */
    /**
     * Reposition the cascader panel. When a menu opens, the cascader expands
     * and may exceed the boundary of browser's window.
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.reposition = /**
     * Reposition the cascader panel. When a menu opens, the cascader expands
     * and may exceed the boundary of browser's window.
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        if (this.overlay && this.overlay.overlayRef && this.menuVisible) {
            Promise.resolve().then((/**
             * @return {?}
             */
            function () {
                _this.overlay.overlayRef.updatePosition();
            }));
        }
    };
    /**
     * When a cascader options is changed, a child needs to know that it should re-render.
     */
    /**
     * When a cascader options is changed, a child needs to know that it should re-render.
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.checkChildren = /**
     * When a cascader options is changed, a child needs to know that it should re-render.
     * @private
     * @return {?}
     */
    function () {
        if (this.cascaderItems) {
            this.cascaderItems.forEach((/**
             * @param {?} item
             * @return {?}
             */
            function (item) { return item.markForCheck(); }));
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCascaderComponent.prototype.buildDisplayLabel = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var selectedOptions = this.cascaderService.selectedOptions;
        /** @type {?} */
        var labels = selectedOptions.map((/**
         * @param {?} o
         * @return {?}
         */
        function (o) { return _this.cascaderService.getOptionLabel(o); }));
        if (this.isLabelRenderTemplate) {
            this.labelRenderContext = { labels: labels, selectedOptions: selectedOptions };
        }
        else {
            this.labelRenderText = defaultDisplayRender.call(this, labels, selectedOptions);
        }
    };
    NzCascaderComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-cascader,[nz-cascader]',
                    preserveWhitespaces: false,
                    template: "<div\n  cdkOverlayOrigin\n  #origin=\"cdkOverlayOrigin\"\n  #trigger>\n  <div *ngIf=\"nzShowInput\">\n    <input\n      #input\n      nz-input\n      class=\"ant-cascader-input\"\n      [class.ant-cascader-input-disabled]=\"nzDisabled\"\n      [class.ant-cascader-input-lg]=\"nzSize === 'large'\"\n      [class.ant-cascader-input-sm]=\"nzSize === 'small'\"\n      [attr.autoComplete]=\"'off'\"\n      [attr.placeholder]=\"showPlaceholder ? nzPlaceHolder : null\"\n      [attr.autofocus]=\"nzAutoFocus ? 'autofocus' : null\"\n      [readonly]=\"!nzShowSearch\"\n      [disabled]=\"nzDisabled\"\n      [nzSize]=\"nzSize\"\n      [(ngModel)]=\"inputValue\"\n      (blur)=\"handleInputBlur()\"\n      (focus)=\"handleInputFocus()\"\n      (change)=\"$event.stopPropagation()\">\n    <i *ngIf=\"clearIconVisible\"\n      nz-icon\n      type=\"close-circle\"\n      theme=\"fill\"\n      class=\"ant-cascader-picker-clear\"\n      (click)=\"clearSelection($event)\"></i>\n    <i *ngIf=\"nzShowArrow && !isLoading\"\n      nz-icon\n      type=\"down\"\n      class=\"ant-cascader-picker-arrow\"\n      [class.ant-cascader-picker-arrow-expand]=\"menuVisible\">\n    </i>\n    <i *ngIf=\"isLoading\" nz-icon type=\"loading\" class=\"ant-cascader-picker-arrow\"></i>\n    <span\n      class=\"ant-cascader-picker-label\"\n      [class.ant-cascader-show-search]=\"!!nzShowSearch\"\n      [class.ant-focusd]=\"!!nzShowSearch && isFocused && !inputValue\">\n      <ng-container *ngIf=\"!isLabelRenderTemplate; else labelTemplate\">{{ labelRenderText }}</ng-container>\n      <ng-template #labelTemplate>\n        <ng-template [ngTemplateOutlet]=\"nzLabelRender\" [ngTemplateOutletContext]=\"labelRenderContext\"></ng-template>\n      </ng-template>\n    </span>\n  </div>\n  <ng-content></ng-content>\n</div>\n<ng-template\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  cdkConnectedOverlayHasBackdrop\n  [cdkConnectedOverlayOrigin]=\"origin\"\n  [cdkConnectedOverlayPositions]=\"positions\"\n  (backdropClick)=\"closeMenu()\"\n  (detach)=\"closeMenu()\"\n  (positionChange)=\"onPositionChange($event)\"\n  [cdkConnectedOverlayOpen]=\"menuVisible\">\n  <div\n    #menu\n    class=\"ant-cascader-menus\"\n    *ngIf=\"nzOptions && nzOptions.length || inSearchingMode\"\n    [class.ant-cascader-menus-hidden]=\"!menuVisible\"\n    [ngClass]=\"menuCls\"\n    [ngStyle]=\"nzMenuStyle\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [@slideMotion]=\"dropDownPosition\"\n    (mouseleave)=\"onTriggerMouseLeave($event)\">\n    <ul *ngFor=\"let options of cascaderService.columns; let i = index;\" class=\"ant-cascader-menu\"\n      [ngClass]=\"menuColumnCls\"\n      [style.height]=\"inSearchingMode && !cascaderService.columns[0].length ? 'auto': ''\"\n      [style.width]=\"dropdownWidthStyle\">\n      <li\n        nz-cascader-option\n        *ngFor=\"let option of options\"\n        [nzLabelProperty]=\"nzLabelProperty\"\n        [activated]=\"isOptionActivated(option, i)\"\n        [highlightText]=\"inSearchingMode ? inputValue : ''\"\n        [option]=\"option\"\n        (mouseenter)=\"onOptionMouseEnter(option, i, $event)\"\n        (mouseleave)=\"onOptionMouseLeave(option, i, $event)\"\n        (click)=\"onOptionClick(option, i, $event)\">\n      </li>\n      <li *ngIf=\"inSearchingMode && !cascaderService.columns[0].length\"\n        class=\"ant-cascader-menu-item ant-cascader-menu-item-expanded ant-cascader-menu-item-disabled\">\n        <nz-embed-empty [nzComponentName]=\"'cascader'\" [specificContent]=\"nzNotFoundContent\"></nz-embed-empty>\n      </li>\n    </ul>\n  </div>\n</ng-template>\n",
                    animations: [slideMotion],
                    providers: [
                        {
                            provide: NG_VALUE_ACCESSOR,
                            useExisting: forwardRef((/**
                             * @return {?}
                             */
                            function () { return NzCascaderComponent; })),
                            multi: true
                        },
                        NzCascaderService
                    ],
                    host: {
                        '[attr.tabIndex]': '"0"',
                        '[class.ant-cascader-lg]': 'nzSize === "large"',
                        '[class.ant-cascader-sm]': 'nzSize === "small"',
                        '[class.ant-cascader-picker-disabled]': 'nzDisabled',
                        '[class.ant-cascader-picker-open]': 'menuVisible',
                        '[class.ant-cascader-picker-with-value]': '!!inputValue',
                        '[class.ant-cascader-focused]': 'isFocused'
                    },
                    styles: ["\n      .ant-cascader-menus {\n        margin-top: 4px;\n        margin-bottom: 4px;\n        top: 100%;\n        left: 0;\n        position: relative;\n        width: 100%;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzCascaderComponent.ctorParameters = function () { return [
        { type: NzCascaderService },
        { type: ChangeDetectorRef },
        { type: ElementRef },
        { type: Renderer2 },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzCascaderComponent.propDecorators = {
        input: [{ type: ViewChild, args: ['input',] }],
        menu: [{ type: ViewChild, args: ['menu',] }],
        overlay: [{ type: ViewChild, args: [CdkConnectedOverlay,] }],
        cascaderItems: [{ type: ViewChildren, args: [NzCascaderOptionComponent,] }],
        nzShowInput: [{ type: Input }],
        nzShowArrow: [{ type: Input }],
        nzAllowClear: [{ type: Input }],
        nzAutoFocus: [{ type: Input }],
        nzChangeOnSelect: [{ type: Input }],
        nzDisabled: [{ type: Input }],
        nzColumnClassName: [{ type: Input }],
        nzExpandTrigger: [{ type: Input }],
        nzValueProperty: [{ type: Input }],
        nzLabelRender: [{ type: Input }],
        nzLabelProperty: [{ type: Input }],
        nzNotFoundContent: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzShowSearch: [{ type: Input }],
        nzPlaceHolder: [{ type: Input }],
        nzMenuClassName: [{ type: Input }],
        nzMenuStyle: [{ type: Input }],
        nzMouseEnterDelay: [{ type: Input }],
        nzMouseLeaveDelay: [{ type: Input }],
        nzTriggerAction: [{ type: Input }],
        nzChangeOn: [{ type: Input }],
        nzLoadData: [{ type: Input }],
        nzOptions: [{ type: Input }],
        nzSelectionChange: [{ type: Output }],
        nzSelect: [{ type: Output }],
        nzClear: [{ type: Output }],
        nzVisibleChange: [{ type: Output }],
        nzChange: [{ type: Output }],
        onKeyDown: [{ type: HostListener, args: ['keydown', ['$event'],] }],
        onTriggerClick: [{ type: HostListener, args: ['click',] }],
        onTriggerMouseEnter: [{ type: HostListener, args: ['mouseenter',] }],
        onTriggerMouseLeave: [{ type: HostListener, args: ['mouseleave', ['$event'],] }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCascaderComponent.prototype, "nzShowInput", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCascaderComponent.prototype, "nzShowArrow", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCascaderComponent.prototype, "nzAllowClear", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCascaderComponent.prototype, "nzAutoFocus", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCascaderComponent.prototype, "nzChangeOnSelect", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCascaderComponent.prototype, "nzDisabled", void 0);
    return NzCascaderComponent;
}());
export { NzCascaderComponent };
if (false) {
    /** @type {?} */
    NzCascaderComponent.prototype.input;
    /** @type {?} */
    NzCascaderComponent.prototype.menu;
    /** @type {?} */
    NzCascaderComponent.prototype.overlay;
    /** @type {?} */
    NzCascaderComponent.prototype.cascaderItems;
    /** @type {?} */
    NzCascaderComponent.prototype.nzShowInput;
    /** @type {?} */
    NzCascaderComponent.prototype.nzShowArrow;
    /** @type {?} */
    NzCascaderComponent.prototype.nzAllowClear;
    /** @type {?} */
    NzCascaderComponent.prototype.nzAutoFocus;
    /** @type {?} */
    NzCascaderComponent.prototype.nzChangeOnSelect;
    /** @type {?} */
    NzCascaderComponent.prototype.nzDisabled;
    /** @type {?} */
    NzCascaderComponent.prototype.nzColumnClassName;
    /** @type {?} */
    NzCascaderComponent.prototype.nzExpandTrigger;
    /** @type {?} */
    NzCascaderComponent.prototype.nzValueProperty;
    /** @type {?} */
    NzCascaderComponent.prototype.nzLabelRender;
    /** @type {?} */
    NzCascaderComponent.prototype.nzLabelProperty;
    /** @type {?} */
    NzCascaderComponent.prototype.nzNotFoundContent;
    /** @type {?} */
    NzCascaderComponent.prototype.nzSize;
    /** @type {?} */
    NzCascaderComponent.prototype.nzShowSearch;
    /** @type {?} */
    NzCascaderComponent.prototype.nzPlaceHolder;
    /** @type {?} */
    NzCascaderComponent.prototype.nzMenuClassName;
    /** @type {?} */
    NzCascaderComponent.prototype.nzMenuStyle;
    /** @type {?} */
    NzCascaderComponent.prototype.nzMouseEnterDelay;
    /** @type {?} */
    NzCascaderComponent.prototype.nzMouseLeaveDelay;
    /** @type {?} */
    NzCascaderComponent.prototype.nzTriggerAction;
    /** @type {?} */
    NzCascaderComponent.prototype.nzChangeOn;
    /** @type {?} */
    NzCascaderComponent.prototype.nzLoadData;
    /** @type {?} */
    NzCascaderComponent.prototype.nzSelectionChange;
    /** @type {?} */
    NzCascaderComponent.prototype.nzSelect;
    /** @type {?} */
    NzCascaderComponent.prototype.nzClear;
    /** @type {?} */
    NzCascaderComponent.prototype.nzVisibleChange;
    /** @type {?} */
    NzCascaderComponent.prototype.nzChange;
    /** @type {?} */
    NzCascaderComponent.prototype.el;
    /** @type {?} */
    NzCascaderComponent.prototype.dropDownPosition;
    /** @type {?} */
    NzCascaderComponent.prototype.menuVisible;
    /** @type {?} */
    NzCascaderComponent.prototype.isLoading;
    /** @type {?} */
    NzCascaderComponent.prototype.labelRenderText;
    /** @type {?} */
    NzCascaderComponent.prototype.labelRenderContext;
    /** @type {?} */
    NzCascaderComponent.prototype.onChange;
    /** @type {?} */
    NzCascaderComponent.prototype.onTouched;
    /** @type {?} */
    NzCascaderComponent.prototype.positions;
    /** @type {?} */
    NzCascaderComponent.prototype.dropdownWidthStyle;
    /** @type {?} */
    NzCascaderComponent.prototype.isFocused;
    /**
     * @type {?}
     * @private
     */
    NzCascaderComponent.prototype.$destroy;
    /**
     * @type {?}
     * @private
     */
    NzCascaderComponent.prototype.inputString;
    /**
     * @type {?}
     * @private
     */
    NzCascaderComponent.prototype.isOpening;
    /**
     * @type {?}
     * @private
     */
    NzCascaderComponent.prototype.delayMenuTimer;
    /**
     * @type {?}
     * @private
     */
    NzCascaderComponent.prototype.delaySelectTimer;
    /** @type {?} */
    NzCascaderComponent.prototype.cascaderService;
    /**
     * @type {?}
     * @private
     */
    NzCascaderComponent.prototype.cdr;
    /** @type {?} */
    NzCascaderComponent.prototype.noAnimation;
}
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FzY2FkZXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhc2NhZGVyL256LWNhc2NhZGVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxTQUFTLEVBQUUsVUFBVSxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsVUFBVSxFQUFFLFdBQVcsRUFBRSxRQUFRLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUNoSCxPQUFPLEVBQUUsbUJBQW1CLEVBQTBELE1BQU0sc0JBQXNCLENBQUM7QUFDbkgsT0FBTyxFQUNMLFVBQVUsRUFDVix1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLElBQUksRUFDSixZQUFZLEVBQ1osS0FBSyxFQUdMLFFBQVEsRUFDUixNQUFNLEVBQ04sU0FBUyxFQUNULFNBQVMsRUFDVCxXQUFXLEVBQ1gsU0FBUyxFQUNULFlBQVksRUFDWixpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUF3QixpQkFBaUIsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQ3pFLE9BQU8sRUFBRSxPQUFPLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFDL0IsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRTNDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUN0RCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSxnREFBZ0QsQ0FBQztBQUN4RixPQUFPLEVBQUUsMEJBQTBCLEVBQUUsTUFBTSxrQ0FBa0MsQ0FBQztBQUU5RSxPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFDN0MsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBV3BELE9BQU8sRUFBRSx5QkFBeUIsRUFBRSxNQUFNLDRC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QkFDS1NQQUNFLCBET1dOX0FSUk9XLCBFTlRFUiwgRVNDQVBFLCBMRUZUX0FSUk9XLCBSSUdIVF9BUlJPVywgVVBfQVJST1cgfSBmcm9tICdAYW5ndWxhci9jZGsva2V5Y29kZXMnO1xuaW1wb3J0IHsgQ2RrQ29ubmVjdGVkT3ZlcmxheSwgQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9uQ2hhbmdlLCBDb25uZWN0aW9uUG9zaXRpb25QYWlyIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHtcbiAgZm9yd2FyZFJlZixcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSG9zdCxcbiAgSG9zdExpc3RlbmVyLFxuICBJbnB1dCxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFF1ZXJ5TGlzdCxcbiAgUmVuZGVyZXIyLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0NoaWxkLFxuICBWaWV3Q2hpbGRyZW4sXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQ29udHJvbFZhbHVlQWNjZXNzb3IsIE5HX1ZBTFVFX0FDQ0VTU09SIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IHsgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQgeyBzbGlkZU1vdGlvbiB9IGZyb20gJy4uL2NvcmUvYW5pbWF0aW9uL3NsaWRlJztcbmltcG9ydCB7IE56Tm9BbmltYXRpb25EaXJlY3RpdmUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24uZGlyZWN0aXZlJztcbmltcG9ydCB7IERFRkFVTFRfQ0FTQ0FERVJfUE9TSVRJT05TIH0gZnJvbSAnLi4vY29yZS9vdmVybGF5L292ZXJsYXktcG9zaXRpb24nO1xuaW1wb3J0IHsgTmdDbGFzc1R5cGUgfSBmcm9tICcuLi9jb3JlL3R5cGVzL25nLWNsYXNzJztcbmltcG9ydCB7IHRvQXJyYXkgfSBmcm9tICcuLi9jb3JlL3V0aWwvYXJyYXknO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuXG5pbXBvcnQge1xuICBDYXNjYWRlck9wdGlvbixcbiAgQ2FzY2FkZXJTZWFyY2hPcHRpb24sXG4gIE56Q2FzY2FkZXJDb21wb25lbnRBc1NvdXJjZSxcbiAgTnpDYXNjYWRlckV4cGFuZFRyaWdnZXIsXG4gIE56Q2FzY2FkZXJTaXplLFxuICBOekNhc2NhZGVyVHJpZ2dlclR5cGUsXG4gIE56U2hvd1NlYXJjaE9wdGlvbnNcbn0gZnJvbSAnLi9uei1jYXNjYWRlci1kZWZpbml0aW9ucyc7XG5pbXBvcnQgeyBOekNhc2NhZGVyT3B0aW9uQ29tcG9uZW50IH0gZnJvbSAnLi9uei1jYXNjYWRlci1saS5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpDYXNjYWRlclNlcnZpY2UgfSBmcm9tICcuL256LWNhc2NhZGVyLnNlcnZpY2UnO1xuXG5jb25zdCBkZWZhdWx0RGlzcGxheVJlbmRlciA9IChsYWJlbHM6IHN0cmluZ1tdKSA9PiBsYWJlbHMuam9pbignIC8gJyk7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1jYXNjYWRlcixbbnotY2FzY2FkZXJdJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1jYXNjYWRlci5jb21wb25lbnQuaHRtbCcsXG4gIGFuaW1hdGlvbnM6IFtzbGlkZU1vdGlvbl0sXG4gIHByb3ZpZGVyczogW1xuICAgIHtcbiAgICAgIHByb3ZpZGU6IE5HX1ZBTFVFX0FDQ0VTU09SLFxuICAgICAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTnpDYXNjYWRlckNvbXBvbmVudCksXG4gICAgICBtdWx0aTogdHJ1ZVxuICAgIH0sXG4gICAgTnpDYXNjYWRlclNlcnZpY2VcbiAgXSxcbiAgaG9zdDoge1xuICAgICdbYXR0ci50YWJJbmRleF0nOiAnXCIwXCInLFxuICAgICdbY2xhc3MuYW50LWNhc2NhZGVyLWxnXSc6ICduelNpemUgPT09IFwibGFyZ2VcIicsXG4gICAgJ1tjbGFzcy5hbnQtY2FzY2FkZXItc21dJzogJ256U2l6ZSA9PT0gXCJzbWFsbFwiJyxcbiAgICAnW2NsYXNzLmFudC1jYXNjYWRlci1waWNrZXItZGlzYWJsZWRdJzogJ256RGlzYWJsZWQnLFxuICAgICdbY2xhc3MuYW50LWNhc2NhZGVyLXBpY2tlci1vcGVuXSc6ICdtZW51VmlzaWJsZScsXG4gICAgJ1tjbGFzcy5hbnQtY2FzY2FkZXItcGlja2VyLXdpdGgtdmFsdWVdJzogJyEhaW5wdXRWYWx1ZScsXG4gICAgJ1tjbGFzcy5hbnQtY2FzY2FkZXItZm9jdXNlZF0nOiAnaXNGb2N1c2VkJ1xuICB9LFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICAuYW50LWNhc2NhZGVyLW1lbnVzIHtcbiAgICAgICAgbWFyZ2luLXRvcDogNHB4O1xuICAgICAgICBtYXJnaW4tYm90dG9tOiA0cHg7XG4gICAgICAgIHRvcDogMTAwJTtcbiAgICAgICAgbGVmdDogMDtcbiAgICAgICAgcG9zaXRpb246IHJlbGF0aXZlO1xuICAgICAgICB3aWR0aDogMTAwJTtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpDYXNjYWRlckNvbXBvbmVudCBpbXBsZW1lbnRzIE56Q2FzY2FkZXJDb21wb25lbnRBc1NvdXJjZSwgT25Jbml0LCBPbkRlc3Ryb3ksIENvbnRyb2xWYWx1ZUFjY2Vzc29yIHtcbiAgQFZpZXdDaGlsZCgnaW5wdXQnKSBpbnB1dDogRWxlbWVudFJlZjtcbiAgQFZpZXdDaGlsZCgnbWVudScpIG1lbnU6IEVsZW1lbnRSZWY7XG4gIEBWaWV3Q2hpbGQoQ2RrQ29ubmVjdGVkT3ZlcmxheSkgb3ZlcmxheTogQ2RrQ29ubmVjdGVkT3ZlcmxheTtcbiAgQFZpZXdDaGlsZHJlbihOekNhc2NhZGVyT3B0aW9uQ29tcG9uZW50KSBjYXNjYWRlckl0ZW1zOiBRdWVyeUxpc3Q8TnpDYXNjYWRlck9wdGlvbkNvbXBvbmVudD47XG5cbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd0lucHV0ID0gdHJ1ZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd0Fycm93ID0gdHJ1ZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56QWxsb3dDbGVhciA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekF1dG9Gb2N1cyA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpDaGFuZ2VPblNlbGVjdCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEaXNhYmxlZCA9IGZhbHNlO1xuICBASW5wdXQoKSBuekNvbHVtbkNsYXNzTmFtZTogc3RyaW5nO1xuICBASW5wdXQoKSBuekV4cGFuZFRyaWdnZXI6IE56Q2FzY2FkZXJFeHBhbmRUcmlnZ2VyID0gJ2NsaWNrJztcbiAgQElucHV0KCkgbnpWYWx1ZVByb3BlcnR5ID0gJ3ZhbHVlJztcbiAgQElucHV0KCkgbnpMYWJlbFJlbmRlcjogVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56TGFiZWxQcm9wZXJ0eSA9ICdsYWJlbCc7XG4gIEBJbnB1dCgpIG56Tm90Rm91bmRDb250ZW50OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpTaXplOiBOekNhc2NhZGVyU2l6ZSA9ICdkZWZhdWx0JztcbiAgQElucHV0KCkgbnpTaG93U2VhcmNoOiBib29sZWFuIHwgTnpTaG93U2VhcmNoT3B0aW9ucztcbiAgQElucHV0KCkgbnpQbGFjZUhvbGRlciA9ICdQbGVhc2Ugc2VsZWN0JzsgLy8gVE9ETzogaTE4bj9cbiAgQElucHV0KCkgbnpNZW51Q2xhc3NOYW1lOiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56TWVudVN0eWxlOiB7IFtrZXk6IHN0cmluZ106IHN0cmluZyB9O1xuICBASW5wdXQoKSBuek1vdXNlRW50ZXJEZWxheTogbnVtYmVyID0gMTUwOyAvLyBtc1xuICBASW5wdXQoKSBuek1vdXNlTGVhdmVEZWxheTogbnVtYmVyID0gMTUwOyAvLyBtc1xuICBASW5wdXQoKSBuelRyaWdnZXJBY3Rpb246IE56Q2FzY2FkZXJUcmlnZ2VyVHlwZSB8IE56Q2FzY2FkZXJUcmlnZ2VyVHlwZVtdID0gWydjbGljayddIGFzIE56Q2FzY2FkZXJUcmlnZ2VyVHlwZVtdO1xuICBASW5wdXQoKSBuekNoYW5nZU9uOiAob3B0aW9uOiBDYXNjYWRlck9wdGlvbiwgbGV2ZWw6IG51bWJlcikgPT4gYm9vbGVhbjtcbiAgQElucHV0KCkgbnpMb2FkRGF0YTogKG5vZGU6IENhc2NhZGVyT3B0aW9uLCBpbmRleD86IG51bWJlcikgPT4gUHJvbWlzZUxpa2U8YW55PjsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcblxuICBASW5wdXQoKVxuICBnZXQgbnpPcHRpb25zKCk6IENhc2NhZGVyT3B0aW9uW10gfCBudWxsIHtcbiAgICByZXR1cm4gdGhpcy5jYXNjYWRlclNlcnZpY2UubnpPcHRpb25zO1xuICB9XG5cbiAgc2V0IG56T3B0aW9ucyhvcHRpb25zOiBDYXNjYWRlck9wdGlvbltdIHwgbnVsbCkge1xuICAgIHRoaXMuY2FzY2FkZXJTZXJ2aWNlLndpdGhPcHRpb25zKG9wdGlvbnMpO1xuICB9XG5cbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56U2VsZWN0aW9uQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxDYXNjYWRlck9wdGlvbltdPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpTZWxlY3QgPSBuZXcgRXZlbnRFbWl0dGVyPHsgb3B0aW9uOiBDYXNjYWRlck9wdGlvbjsgaW5kZXg6IG51bWJlciB9IHwgbnVsbD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Q2xlYXIgPSBuZXcgRXZlbnRFbWl0dGVyPHZvaWQ+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelZpc2libGVDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPGJvb2xlYW4+KCk7IC8vIE5vdCBleHBvc2VkLCBvbmx5IGZvciB0ZXN0XG4gIEBPdXRwdXQoKSByZWFkb25seSBuekNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXIoKTsgLy8gTm90IGV4cG9zZWQsIG9ubHkgZm9yIHRlc3RcblxuICBlbDogSFRNTEVsZW1lbnQ7XG4gIGRyb3BEb3duUG9zaXRpb24gPSAnYm90dG9tJztcbiAgbWVudVZpc2libGUgPSBmYWxzZTtcbiAgaXNMb2FkaW5nID0gZmFsc2U7XG4gIGxhYmVsUmVuZGVyVGV4dDogc3RyaW5nO1xuICBsYWJlbFJlbmRlckNvbnRleHQgPSB7fTtcbiAgb25DaGFuZ2UgPSBGdW5jdGlvbi5wcm90b3R5cGU7XG4gIG9uVG91Y2hlZCA9IEZ1bmN0aW9uLnByb3RvdHlwZTtcbiAgcG9zaXRpb25zOiBDb25uZWN0aW9uUG9zaXRpb25QYWlyW10gPSBbLi4uREVGQVVMVF9DQVNDQURFUl9QT1NJVElPTlNdO1xuICBkcm9wZG93bldpZHRoU3R5bGU6IHN0cmluZztcbiAgaXNGb2N1c2VkID0gZmFsc2U7XG5cbiAgcHJpdmF0ZSAkZGVzdHJveSA9IG5ldyBTdWJqZWN0PHZvaWQ+KCk7XG4gIHByaXZhdGUgaW5wdXRTdHJpbmcgPSAnJztcbiAgcHJpdmF0ZSBpc09wZW5pbmcgPSBmYWxzZTtcbiAgcHJpdmF0ZSBkZWxheU1lbnVUaW1lcjogbnVtYmVyIHwgbnVsbDtcbiAgcHJpdmF0ZSBkZWxheVNlbGVjdFRpbWVyOiBudW1iZXIgfCBudWxsO1xuXG4gIGdldCBpblNlYXJjaGluZ01vZGUoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmluU2VhcmNoaW5nTW9kZTtcbiAgfVxuXG4gIHNldCBpbnB1dFZhbHVlKGlucHV0VmFsdWU6IHN0cmluZykge1xuICAgIHRoaXMuaW5wdXRTdHJpbmcgPSBpbnB1dFZhbHVlO1xuICAgIHRoaXMudG9nZ2xlU2VhcmNoaW5nTW9kZSghIWlucHV0VmFsdWUpO1xuICB9XG5cbiAgZ2V0IGlucHV0VmFsdWUoKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5pbnB1dFN0cmluZztcbiAgfVxuXG4gIGdldCBtZW51Q2xzKCk6IE5nQ2xhc3NUeXBlIHtcbiAgICByZXR1cm4geyBbYCR7dGhpcy5uek1lbnVDbGFzc05hbWV9YF06ICEhdGhpcy5uek1lbnVDbGFzc05hbWUgfTtcbiAgfVxuXG4gIGdldCBtZW51Q29sdW1uQ2xzKCk6IE5nQ2xhc3NUeXBlIHtcbiAgICByZXR1cm4geyBbYCR7dGhpcy5uekNvbHVtbkNsYXNzTmFtZX1gXTogISF0aGlzLm56Q29sdW1uQ2xhc3NOYW1lIH07XG4gIH1cblxuICBwcml2YXRlIGdldCBoYXNJbnB1dCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gISF0aGlzLmlucHV0VmFsdWU7XG4gIH1cblxuICBwcml2YXRlIGdldCBoYXNWYWx1ZSgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5jYXNjYWRlclNlcnZpY2UudmFsdWVzICYmIHRoaXMuY2FzY2FkZXJTZXJ2aWNlLnZhbHVlcy5sZW5ndGggPiAwO1xuICB9XG5cbiAgZ2V0IHNob3dQbGFjZWhvbGRlcigpOiBib29sZWFuIHtcbiAgICByZXR1cm4gISh0aGlzLmhhc0lucHV0IHx8IHRoaXMuaGFzVmFsdWUpO1xuICB9XG5cbiAgZ2V0IGNsZWFySWNvblZpc2libGUoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMubnpBbGxvd0NsZWFyICYmICF0aGlzLm56RGlzYWJsZWQgJiYgKHRoaXMuaGFzVmFsdWUgfHwgdGhpcy5oYXNJbnB1dCk7XG4gIH1cblxuICBnZXQgaXNMYWJlbFJlbmRlclRlbXBsYXRlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAhIXRoaXMubnpMYWJlbFJlbmRlcjtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHB1YmxpYyBjYXNjYWRlclNlcnZpY2U6IE56Q2FzY2FkZXJTZXJ2aWNlLFxuICAgIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgQEhvc3QoKSBAT3B0aW9uYWwoKSBwdWJsaWMgbm9BbmltYXRpb24/OiBOek5vQW5pbWF0aW9uRGlyZWN0aXZlXG4gICkge1xuICAgIHRoaXMuZWwgPSBlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQ7XG4gICAgdGhpcy5jYXNjYWRlclNlcnZpY2Uud2l0aENvbXBvbmVudCh0aGlzKTtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtY2FzY2FkZXInKTtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtY2FzY2FkZXItcGlja2VyJyk7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICBjb25zdCBzcnYgPSB0aGlzLmNhc2NhZGVyU2VydmljZTtcblxuICAgIHNydi4kcmVkcmF3LnBpcGUodGFrZVVudGlsKHRoaXMuJGRlc3Ryb3kpKS5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgLy8gVGhlc2Ugb3BlcmF0aW9ucyB3b3VsZCBub3QgbXV0YXRlIGRhdGEuXG4gICAgICB0aGlzLmNoZWNrQ2hpbGRyZW4oKTtcbiAgICAgIHRoaXMuYnVpbGREaXNwbGF5TGFiZWwoKTtcbiAgICAgIHRoaXMucmVwb3NpdGlvbigpO1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfSk7XG5cbiAgICBzcnYuJGxvYWRpbmcucGlwZSh0YWtlVW50aWwodGhpcy4kZGVzdHJveSkpLnN1YnNjcmliZShsb2FkaW5nID0+IHtcbiAgICAgIHRoaXMuaXNMb2FkaW5nID0gbG9hZGluZztcbiAgICB9KTtcblxuICAgIHNydi4kb3B0aW9uU2VsZWN0ZWQucGlwZSh0YWtlVW50aWwodGhpcy4kZGVzdHJveSkpLnN1YnNjcmliZShkYXRhID0+IHtcbiAgICAgIGlmICghZGF0YSkge1xuICAgICAgICB0aGlzLm9uQ2hhbmdlKFtdKTtcbiAgICAgICAgdGhpcy5uelNlbGVjdC5lbWl0KG51bGwpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc3QgeyBvcHRpb24sIGluZGV4IH0gPSBkYXRhO1xuICAgICAgICBjb25zdCBzaG91bGRDbG9zZSA9IG9wdGlvbi5pc0xlYWY7XG4gICAgICAgIGlmIChzaG91bGRDbG9zZSkge1xuICAgICAgICAgIHRoaXMuZGVsYXlTZXRNZW51VmlzaWJsZShmYWxzZSk7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5vbkNoYW5nZSh0aGlzLmNhc2NhZGVyU2VydmljZS52YWx1ZXMpO1xuICAgICAgICB0aGlzLm56U2VsZWN0aW9uQ2hhbmdlLmVtaXQodGhpcy5jYXNjYWRlclNlcnZpY2Uuc2VsZWN0ZWRPcHRpb25zKTtcbiAgICAgICAgdGhpcy5uelNlbGVjdC5lbWl0KHsgb3B0aW9uLCBpbmRleCB9KTtcbiAgICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBzcnYuJHF1aXRTZWFyY2hpbmcucGlwZSh0YWtlVW50aWwodGhpcy4kZGVzdHJveSkpLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICB0aGlzLmlucHV0U3RyaW5nID0gJyc7XG4gICAgICB0aGlzLmRyb3Bkb3duV2lkdGhTdHlsZSA9ICcnO1xuICAgIH0pO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5jbGVhckRlbGF5TWVudVRpbWVyKCk7XG4gICAgdGhpcy5jbGVhckRlbGF5U2VsZWN0VGltZXIoKTtcbiAgfVxuXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46ICgpID0+IHt9KTogdm9pZCB7XG4gICAgdGhpcy5vbkNoYW5nZSA9IGZuO1xuICB9XG5cbiAgcmVnaXN0ZXJPblRvdWNoZWQoZm46ICgpID0+IHt9KTogdm9pZCB7XG4gICAgdGhpcy5vblRvdWNoZWQgPSBmbjtcbiAgfVxuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgd3JpdGVWYWx1ZSh2YWx1ZTogYW55KTogdm9pZCB7XG4gICAgdGhpcy5jYXNjYWRlclNlcnZpY2UudmFsdWVzID0gdG9BcnJheSh2YWx1ZSk7XG4gICAgdGhpcy5jYXNjYWRlclNlcnZpY2Uuc3luY09wdGlvbnModHJ1ZSk7XG4gIH1cblxuICBkZWxheVNldE1lbnVWaXNpYmxlKHZpc2libGU6IGJvb2xlYW4sIGRlbGF5OiBudW1iZXIgPSAxMDAsIHNldE9wZW5pbmc6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIHRoaXMuY2xlYXJEZWxheU1lbnVUaW1lcigpO1xuICAgIGlmIChkZWxheSkge1xuICAgICAgaWYgKHZpc2libGUgJiYgc2V0T3BlbmluZykge1xuICAgICAgICB0aGlzLmlzT3BlbmluZyA9IHRydWU7XG4gICAgICB9XG4gICAgICB0aGlzLmRlbGF5TWVudVRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuc2V0TWVudVZpc2libGUodmlzaWJsZSk7XG4gICAgICAgIHRoaXMuY2RyLmRldGVjdENoYW5nZXMoKTtcbiAgICAgICAgdGhpcy5jbGVhckRlbGF5TWVudVRpbWVyKCk7XG4gICAgICAgIGlmICh2aXNpYmxlKSB7XG4gICAgICAgICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgICAgICB0aGlzLmlzT3BlbmluZyA9IGZhbHNlO1xuICAgICAgICAgIH0sIDEwMCk7XG4gICAgICAgIH1cbiAgICAgIH0sIGRlbGF5KTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5zZXRNZW51VmlzaWJsZSh2aXNpYmxlKTtcbiAgICB9XG4gIH1cblxuICBzZXRNZW51VmlzaWJsZSh2aXNpYmxlOiBib29sZWFuKTogdm9pZCB7XG4gICAgaWYgKHRoaXMubnpEaXNhYmxlZCB8fCB0aGlzLm1lbnVWaXNpYmxlID09PSB2aXNpYmxlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmICh2aXNpYmxlKSB7XG4gICAgICB0aGlzLmNhc2NhZGVyU2VydmljZS5zeW5jT3B0aW9ucygpO1xuICAgIH1cblxuICAgIHRoaXMubWVudVZpc2libGUgPSB2aXNpYmxlO1xuICAgIHRoaXMubnpWaXNpYmxlQ2hhbmdlLmVtaXQodmlzaWJsZSk7XG4gICAgdGhpcy5jZHIuZGV0ZWN0Q2hhbmdlcygpO1xuICB9XG5cbiAgcHJpdmF0ZSBjbGVhckRlbGF5TWVudVRpbWVyKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmRlbGF5TWVudVRpbWVyKSB7XG4gICAgICBjbGVhclRpbWVvdXQodGhpcy5kZWxheU1lbnVUaW1lcik7XG4gICAgICB0aGlzLmRlbGF5TWVudVRpbWVyID0gbnVsbDtcbiAgICB9XG4gIH1cblxuICBjbGVhclNlbGVjdGlvbihldmVudD86IEV2ZW50KTogdm9pZCB7XG4gICAgaWYgKGV2ZW50KSB7XG4gICAgICBldmVudC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgfVxuXG4gICAgdGhpcy5sYWJlbFJlbmRlclRleHQgPSAnJztcbiAgICB0aGlzLmxhYmVsUmVuZGVyQ29udGV4dCA9IHt9O1xuICAgIHRoaXMuaW5wdXRWYWx1ZSA9ICcnO1xuICAgIHRoaXMuc2V0TWVudVZpc2libGUoZmFsc2UpO1xuICAgIHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmNsZWFyKCk7XG4gIH1cblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGdldFN1Ym1pdFZhbHVlKCk6IGFueVtdIHtcbiAgICByZXR1cm4gdGhpcy5jYXNjYWRlclNlcnZpY2Uuc2VsZWN0ZWRPcHRpb25zLm1hcChvID0+IHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmdldE9wdGlvblZhbHVlKG8pKTtcbiAgfVxuXG4gIGZvY3VzKCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5pc0ZvY3VzZWQpIHtcbiAgICAgICh0aGlzLmlucHV0ID8gdGhpcy5pbnB1dC5uYXRpdmVFbGVtZW50IDogdGhpcy5lbCkuZm9jdXMoKTtcbiAgICAgIHRoaXMuaXNGb2N1c2VkID0gdHJ1ZTtcbiAgICB9XG4gIH1cblxuICBibHVyKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmlzRm9jdXNlZCkge1xuICAgICAgKHRoaXMuaW5wdXQgPyB0aGlzLmlucHV0Lm5hdGl2ZUVsZW1lbnQgOiB0aGlzLmVsKS5ibHVyKCk7XG4gICAgICB0aGlzLmlzRm9jdXNlZCA9IGZhbHNlO1xuICAgIH1cbiAgfVxuXG4gIGhhbmRsZUlucHV0Qmx1cigpOiB2b2lkIHtcbiAgICB0aGlzLm1lbnVWaXNpYmxlID8gdGhpcy5mb2N1cygpIDogdGhpcy5ibHVyKCk7XG4gIH1cblxuICBoYW5kbGVJbnB1dEZvY3VzKCk6IHZvaWQge1xuICAgIHRoaXMuZm9jdXMoKTtcbiAgfVxuXG4gIEBIb3N0TGlzdGVuZXIoJ2tleWRvd24nLCBbJyRldmVudCddKVxuICBvbktleURvd24oZXZlbnQ6IEtleWJvYXJkRXZlbnQpOiB2b2lkIHtcbiAgICBjb25zdCBrZXlDb2RlID0gZXZlbnQua2V5Q29kZTtcblxuICAgIGlmIChcbiAgICAgIGtleUNvZGUgIT09IERPV05fQVJST1cgJiZcbiAgICAgIGtleUNvZGUgIT09IFVQX0FSUk9XICYmXG4gICAgICBrZXlDb2RlICE9PSBMRUZUX0FSUk9XICYmXG4gICAgICBrZXlDb2RlICE9PSBSSUdIVF9BUlJPVyAmJlxuICAgICAga2V5Q29kZSAhPT0gRU5URVIgJiZcbiAgICAgIGtleUNvZGUgIT09IEJBQ0tTUEFDRSAmJlxuICAgICAga2V5Q29kZSAhPT0gRVNDQVBFXG4gICAgKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gUHJlc3MgYW55IGtleXMgYWJvdmUgdG8gcmVvcGVuIG1lbnUuXG4gICAgaWYgKCF0aGlzLm1lbnVWaXNpYmxlICYmIGtleUNvZGUgIT09IEJBQ0tTUEFDRSAmJiBrZXlDb2RlICE9PSBFU0NBUEUpIHtcbiAgICAgIHJldHVybiB0aGlzLnNldE1lbnVWaXNpYmxlKHRydWUpO1xuICAgIH1cblxuICAgIC8vIE1ha2UgdGhlc2Uga2V5cyB3b3JrIGFzIGRlZmF1bHQgaW4gc2VhcmNoaW5nIG1vZGUuXG4gICAgaWYgKHRoaXMuaW5TZWFyY2hpbmdNb2RlICYmIChrZXlDb2RlID09PSBCQUNLU1BBQ0UgfHwga2V5Q29kZSA9PT0gTEVGVF9BUlJPVyB8fCBrZXlDb2RlID09PSBSSUdIVF9BUlJPVykpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBJbnRlcmFjdCB3aXRoIHRoZSBjb21wb25lbnQuXG4gICAgaWYgKHRoaXMubWVudVZpc2libGUpIHtcbiAgICAgIGV2ZW50LnByZXZlbnREZWZhdWx0KCk7XG4gICAgICBpZiAoa2V5Q29kZSA9PT0gRE9XTl9BUlJPVykge1xuICAgICAgICB0aGlzLm1vdmVVcE9yRG93bihmYWxzZSk7XG4gICAgICB9IGVsc2UgaWYgKGtleUNvZGUgPT09IFVQX0FSUk9XKSB7XG4gICAgICAgIHRoaXMubW92ZVVwT3JEb3duKHRydWUpO1xuICAgICAgfSBlbHNlIGlmIChrZXlDb2RlID09PSBMRUZUX0FSUk9XKSB7XG4gICAgICAgIHRoaXMubW92ZUxlZnQoKTtcbiAgICAgIH0gZWxzZSBpZiAoa2V5Q29kZSA9PT0gUklHSFRfQVJST1cpIHtcbiAgICAgICAgdGhpcy5tb3ZlUmlnaHQoKTtcbiAgICAgIH0gZWxzZSBpZiAoa2V5Q29kZSA9PT0gRU5URVIpIHtcbiAgICAgICAgdGhpcy5vbkVudGVyKCk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgQEhvc3RMaXN0ZW5lcignY2xpY2snKVxuICBvblRyaWdnZXJDbGljaygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmICh0aGlzLm56U2hvd1NlYXJjaCkge1xuICAgICAgdGhpcy5mb2N1cygpO1xuICAgIH1cbiAgICBpZiAodGhpcy5pc0FjdGlvblRyaWdnZXIoJ2NsaWNrJykpIHtcbiAgICAgIHRoaXMuZGVsYXlTZXRNZW51VmlzaWJsZSghdGhpcy5tZW51VmlzaWJsZSwgMTAwKTtcbiAgICB9XG4gICAgdGhpcy5vblRvdWNoZWQoKTtcbiAgfVxuXG4gIEBIb3N0TGlzdGVuZXIoJ21vdXNlZW50ZXInKVxuICBvblRyaWdnZXJNb3VzZUVudGVyKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56RGlzYWJsZWQpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgaWYgKHRoaXMuaXNBY3Rpb25UcmlnZ2VyKCdob3ZlcicpKSB7XG4gICAgICB0aGlzLmRlbGF5U2V0TWVudVZpc2libGUodHJ1ZSwgdGhpcy5uek1vdXNlRW50ZXJEZWxheSwgdHJ1ZSk7XG4gICAgfVxuICB9XG5cbiAgQEhvc3RMaXN0ZW5lcignbW91c2VsZWF2ZScsIFsnJGV2ZW50J10pXG4gIG9uVHJpZ2dlck1vdXNlTGVhdmUoZXZlbnQ6IE1vdXNlRXZlbnQpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmICghdGhpcy5tZW51VmlzaWJsZSB8fCB0aGlzLmlzT3BlbmluZykge1xuICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgaWYgKHRoaXMuaXNBY3Rpb25UcmlnZ2VyKCdob3ZlcicpKSB7XG4gICAgICBjb25zdCBtb3VzZVRhcmdldCA9IGV2ZW50LnJlbGF0ZWRUYXJnZXQgYXMgSFRNTEVsZW1lbnQ7XG4gICAgICBjb25zdCBob3N0RWwgPSB0aGlzLmVsO1xuICAgICAgY29uc3QgbWVudUVsID0gdGhpcy5tZW51ICYmICh0aGlzLm1lbnUubmF0aXZlRWxlbWVudCBhcyBIVE1MRWxlbWVudCk7XG4gICAgICBpZiAoaG9zdEVsLmNvbnRhaW5zKG1vdXNlVGFyZ2V0KSB8fCAobWVudUVsICYmIG1lbnVFbC5jb250YWlucyhtb3VzZVRhcmdldCkpKSB7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICAgIHRoaXMuZGVsYXlTZXRNZW51VmlzaWJsZShmYWxzZSwgdGhpcy5uek1vdXNlTGVhdmVEZWxheSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBpc0FjdGlvblRyaWdnZXIoYWN0aW9uOiAnY2xpY2snIHwgJ2hvdmVyJyk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0eXBlb2YgdGhpcy5uelRyaWdnZXJBY3Rpb24gPT09ICdzdHJpbmcnXG4gICAgICA/IHRoaXMubnpUcmlnZ2VyQWN0aW9uID09PSBhY3Rpb25cbiAgICAgIDogdGhpcy5uelRyaWdnZXJBY3Rpb24uaW5kZXhPZihhY3Rpb24pICE9PSAtMTtcbiAgfVxuXG4gIG9uT3B0aW9uQ2xpY2sob3B0aW9uOiBDYXNjYWRlck9wdGlvbiwgY29sdW1uSW5kZXg6IG51bWJlciwgZXZlbnQ6IEV2ZW50KTogdm9pZCB7XG4gICAgaWYgKGV2ZW50KSB7XG4gICAgICBldmVudC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIH1cbiAgICBpZiAob3B0aW9uICYmIG9wdGlvbi5kaXNhYmxlZCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLmVsLmZvY3VzKCk7XG4gICAgdGhpcy5pblNlYXJjaGluZ01vZGVcbiAgICAgID8gdGhpcy5jYXNjYWRlclNlcnZpY2Uuc2V0U2VhcmNoT3B0aW9uU2VsZWN0ZWQob3B0aW9uIGFzIENhc2NhZGVyU2VhcmNoT3B0aW9uKVxuICAgICAgOiB0aGlzLmNhc2NhZGVyU2VydmljZS5zZXRPcHRpb25BY3RpdmF0ZWQob3B0aW9uLCBjb2x1bW5JbmRleCwgdHJ1ZSk7XG4gIH1cblxuICBwcml2YXRlIG9uRW50ZXIoKTogdm9pZCB7XG4gICAgY29uc3QgY29sdW1uSW5kZXggPSBNYXRoLm1heCh0aGlzLmNhc2NhZGVyU2VydmljZS5hY3RpdmF0ZWRPcHRpb25zLmxlbmd0aCAtIDEsIDApO1xuICAgIGNvbnN0IG9wdGlvbiA9IHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmFjdGl2YXRlZE9wdGlvbnNbY29sdW1uSW5kZXhdO1xuICAgIGlmIChvcHRpb24gJiYgIW9wdGlvbi5kaXNhYmxlZCkge1xuICAgICAgdGhpcy5pblNlYXJjaGluZ01vZGVcbiAgICAgICAgPyB0aGlzLmNhc2NhZGVyU2VydmljZS5zZXRTZWFyY2hPcHRpb25TZWxlY3RlZChvcHRpb24gYXMgQ2FzY2FkZXJTZWFyY2hPcHRpb24pXG4gICAgICAgIDogdGhpcy5jYXNjYWRlclNlcnZpY2Uuc2V0T3B0aW9uQWN0aXZhdGVkKG9wdGlvbiwgY29sdW1uSW5kZXgsIHRydWUpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgbW92ZVVwT3JEb3duKGlzVXA6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBjb25zdCBjb2x1bW5JbmRleCA9IE1hdGgubWF4KHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmFjdGl2YXRlZE9wdGlvbnMubGVuZ3RoIC0gMSwgMCk7XG4gICAgY29uc3QgYWN0aXZlT3B0aW9uID0gdGhpcy5jYXNjYWRlclNlcnZpY2UuYWN0aXZhdGVkT3B0aW9uc1tjb2x1bW5JbmRleF07XG4gICAgY29uc3Qgb3B0aW9ucyA9IHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmNvbHVtbnNbY29sdW1uSW5kZXhdIHx8IFtdO1xuICAgIGNvbnN0IGxlbmd0aCA9IG9wdGlvbnMubGVuZ3RoO1xuICAgIGxldCBuZXh0SW5kZXggPSAtMTtcbiAgICBpZiAoIWFjdGl2ZU9wdGlvbikge1xuICAgICAgLy8gTm90IHNlbGVjdGVkIG9wdGlvbnMgaW4gdGhpcyBjb2x1bW5cbiAgICAgIG5leHRJbmRleCA9IGlzVXAgPyBsZW5ndGggOiAtMTtcbiAgICB9IGVsc2Uge1xuICAgICAgbmV4dEluZGV4ID0gb3B0aW9ucy5pbmRleE9mKGFjdGl2ZU9wdGlvbik7XG4gICAgfVxuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIG5leHRJbmRleCA9IGlzVXAgPyBuZXh0SW5kZXggLSAxIDogbmV4dEluZGV4ICsgMTtcbiAgICAgIGlmIChuZXh0SW5kZXggPCAwIHx8IG5leHRJbmRleCA+PSBsZW5ndGgpIHtcbiAgICAgICAgYnJlYWs7XG4gICAgICB9XG4gICAgICBjb25zdCBuZXh0T3B0aW9uID0gb3B0aW9uc1tuZXh0SW5kZXhdO1xuICAgICAgaWYgKCFuZXh0T3B0aW9uIHx8IG5leHRPcHRpb24uZGlzYWJsZWQpIHtcbiAgICAgICAgY29udGludWU7XG4gICAgICB9XG4gICAgICB0aGlzLmNhc2NhZGVyU2VydmljZS5zZXRPcHRpb25BY3RpdmF0ZWQobmV4dE9wdGlvbiwgY29sdW1uSW5kZXgpO1xuICAgICAgYnJlYWs7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBtb3ZlTGVmdCgpOiB2b2lkIHtcbiAgICBjb25zdCBvcHRpb25zID0gdGhpcy5jYXNjYWRlclNlcnZpY2UuYWN0aXZhdGVkT3B0aW9ucztcbiAgICBpZiAob3B0aW9ucy5sZW5ndGgpIHtcbiAgICAgIG9wdGlvbnMucG9wKCk7IC8vIFJlbW92ZSB0aGUgbGFzdCBvbmVcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIG1vdmVSaWdodCgpOiB2b2lkIHtcbiAgICBjb25zdCBsZW5ndGggPSB0aGlzLmNhc2NhZGVyU2VydmljZS5hY3RpdmF0ZWRPcHRpb25zLmxlbmd0aDtcbiAgICBjb25zdCBvcHRpb25zID0gdGhpcy5jYXNjYWRlclNlcnZpY2UuY29sdW1uc1tsZW5ndGhdO1xuICAgIGlmIChvcHRpb25zICYmIG9wdGlvbnMubGVuZ3RoKSB7XG4gICAgICBjb25zdCBuZXh0T3B0ID0gb3B0aW9ucy5maW5kKG8gPT4gIW8uZGlzYWJsZWQpO1xuICAgICAgaWYgKG5leHRPcHQpIHtcbiAgICAgICAgdGhpcy5jYXNjYWRlclNlcnZpY2Uuc2V0T3B0aW9uQWN0aXZhdGVkKG5leHRPcHQsIGxlbmd0aCk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgb25PcHRpb25Nb3VzZUVudGVyKG9wdGlvbjogQ2FzY2FkZXJPcHRpb24sIGNvbHVtbkluZGV4OiBudW1iZXIsIGV2ZW50OiBFdmVudCk6IHZvaWQge1xuICAgIGV2ZW50LnByZXZlbnREZWZhdWx0KCk7XG4gICAgaWYgKHRoaXMubnpFeHBhbmRUcmlnZ2VyID09PSAnaG92ZXInICYmICFvcHRpb24uaXNMZWFmKSB7XG4gICAgICB0aGlzLmRlbGF5U2VsZWN0T3B0aW9uKG9wdGlvbiwgY29sdW1uSW5kZXgsIHRydWUpO1xuICAgIH1cbiAgfVxuXG4gIG9uT3B0aW9uTW91c2VMZWF2ZShvcHRpb246IENhc2NhZGVyT3B0aW9uLCBjb2x1bW5JbmRleDogbnVtYmVyLCBldmVudDogRXZlbnQpOiB2b2lkIHtcbiAgICBldmVudC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIGlmICh0aGlzLm56RXhwYW5kVHJpZ2dlciA9PT0gJ2hvdmVyJyAmJiAhb3B0aW9uLmlzTGVhZikge1xuICAgICAgdGhpcy5kZWxheVNlbGVjdE9wdGlvbihvcHRpb24sIGNvbHVtbkluZGV4LCBmYWxzZSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjbGVhckRlbGF5U2VsZWN0VGltZXIoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuZGVsYXlTZWxlY3RUaW1lcikge1xuICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZGVsYXlTZWxlY3RUaW1lcik7XG4gICAgICB0aGlzLmRlbGF5U2VsZWN0VGltZXIgPSBudWxsO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZGVsYXlTZWxlY3RPcHRpb24ob3B0aW9uOiBDYXNjYWRlck9wdGlvbiwgaW5kZXg6IG51bWJlciwgZG9TZWxlY3Q6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLmNsZWFyRGVsYXlTZWxlY3RUaW1lcigpO1xuICAgIGlmIChkb1NlbGVjdCkge1xuICAgICAgdGhpcy5kZWxheVNlbGVjdFRpbWVyID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuY2FzY2FkZXJTZXJ2aWNlLnNldE9wdGlvbkFjdGl2YXRlZChvcHRpb24sIGluZGV4KTtcbiAgICAgICAgdGhpcy5kZWxheVNlbGVjdFRpbWVyID0gbnVsbDtcbiAgICAgIH0sIDE1MCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSB0b2dnbGVTZWFyY2hpbmdNb2RlKHRvU2VhcmNoaW5nOiBib29sZWFuKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaW5TZWFyY2hpbmdNb2RlICE9PSB0b1NlYXJjaGluZykge1xuICAgICAgdGhpcy5jYXNjYWRlclNlcnZpY2UudG9nZ2xlU2VhcmNoaW5nTW9kZSh0b1NlYXJjaGluZyk7XG4gICAgICB0aGlzLmRyb3Bkb3duV2lkdGhTdHlsZSA9IHRvU2VhcmNoaW5nID8gYCR7dGhpcy5pbnB1dC5uYXRpdmVFbGVtZW50Lm9mZnNldFdpZHRofXB4YCA6ICcnO1xuICAgIH1cbiAgICBpZiAodGhpcy5pblNlYXJjaGluZ01vZGUpIHtcbiAgICAgIHRoaXMuY2FzY2FkZXJTZXJ2aWNlLnByZXBhcmVTZWFyY2hPcHRpb25zKHRoaXMuaW5wdXRWYWx1ZSk7XG4gICAgfVxuICB9XG5cbiAgaXNPcHRpb25BY3RpdmF0ZWQob3B0aW9uOiBDYXNjYWRlck9wdGlvbiwgaW5kZXg6IG51bWJlcik6IGJvb2xlYW4ge1xuICAgIGNvbnN0IGFjdGl2ZU9wdCA9IHRoaXMuY2FzY2FkZXJTZXJ2aWNlLmFjdGl2YXRlZE9wdGlvbnNbaW5kZXhdO1xuICAgIHJldHVybiBhY3RpdmVPcHQgPT09IG9wdGlvbjtcbiAgfVxuXG4gIHNldERpc2FibGVkU3RhdGUoaXNEaXNhYmxlZDogYm9vbGVhbik6IHZvaWQge1xuICAgIGlmIChpc0Rpc2FibGVkKSB7XG4gICAgICB0aGlzLmNsb3NlTWVudSgpO1xuICAgIH1cbiAgICB0aGlzLm56RGlzYWJsZWQgPSBpc0Rpc2FibGVkO1xuICB9XG5cbiAgY2xvc2VNZW51KCk6IHZvaWQge1xuICAgIHRoaXMuYmx1cigpO1xuICAgIHRoaXMuY2xlYXJEZWxheU1lbnVUaW1lcigpO1xuICAgIHRoaXMuc2V0TWVudVZpc2libGUoZmFsc2UpO1xuICB9XG5cbiAgb25Qb3NpdGlvbkNoYW5nZShwb3NpdGlvbjogQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9uQ2hhbmdlKTogdm9pZCB7XG4gICAgY29uc3QgbmV3VmFsdWUgPSBwb3NpdGlvbi5jb25uZWN0aW9uUGFpci5vcmlnaW5ZID09PSAnYm90dG9tJyA/ICdib3R0b20nIDogJ3RvcCc7XG4gICAgaWYgKHRoaXMuZHJvcERvd25Qb3NpdGlvbiAhPT0gbmV3VmFsdWUpIHtcbiAgICAgIHRoaXMuZHJvcERvd25Qb3NpdGlvbiA9IG5ld1ZhbHVlO1xuICAgICAgdGhpcy5jZHIuZGV0ZWN0Q2hhbmdlcygpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBSZXBvc2l0aW9uIHRoZSBjYXNjYWRlciBwYW5lbC4gV2hlbiBhIG1lbnUgb3BlbnMsIHRoZSBjYXNjYWRlciBleHBhbmRzXG4gICAqIGFuZCBtYXkgZXhjZWVkIHRoZSBib3VuZGFyeSBvZiBicm93c2VyJ3Mgd2luZG93LlxuICAgKi9cbiAgcHJpdmF0ZSByZXBvc2l0aW9uKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm92ZXJsYXkgJiYgdGhpcy5vdmVybGF5Lm92ZXJsYXlSZWYgJiYgdGhpcy5tZW51VmlzaWJsZSkge1xuICAgICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB7XG4gICAgICAgIHRoaXMub3ZlcmxheS5vdmVybGF5UmVmLnVwZGF0ZVBvc2l0aW9uKCk7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogV2hlbiBhIGNhc2NhZGVyIG9wdGlvbnMgaXMgY2hhbmdlZCwgYSBjaGlsZCBuZWVkcyB0byBrbm93IHRoYXQgaXQgc2hvdWxkIHJlLXJlbmRlci5cbiAgICovXG4gIHByaXZhdGUgY2hlY2tDaGlsZHJlbigpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5jYXNjYWRlckl0ZW1zKSB7XG4gICAgICB0aGlzLmNhc2NhZGVySXRlbXMuZm9yRWFjaChpdGVtID0+IGl0ZW0ubWFya0ZvckNoZWNrKCkpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgYnVpbGREaXNwbGF5TGFiZWwoKTogdm9pZCB7XG4gICAgY29uc3Qgc2VsZWN0ZWRPcHRpb25zID0gdGhpcy5jYXNjYWRlclNlcnZpY2Uuc2VsZWN0ZWRPcHRpb25zO1xuICAgIGNvbnN0IGxhYmVsczogc3RyaW5nW10gPSBzZWxlY3RlZE9wdGlvbnMubWFwKG8gPT4gdGhpcy5jYXNjYWRlclNlcnZpY2UuZ2V0T3B0aW9uTGFiZWwobykpO1xuXG4gICAgaWYgKHRoaXMuaXNMYWJlbFJlbmRlclRlbXBsYXRlKSB7XG4gICAgICB0aGlzLmxhYmVsUmVuZGVyQ29udGV4dCA9IHsgbGFiZWxzLCBzZWxlY3RlZE9wdGlvbnMgfTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5sYWJlbFJlbmRlclRleHQgPSBkZWZhdWx0RGlzcGxheVJlbmRlci5jYWxsKHRoaXMsIGxhYmVscywgc2VsZWN0ZWRPcHRpb25zKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==