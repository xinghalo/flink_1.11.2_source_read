/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { DOWN_ARROW, ENTER, ESCAPE, LEFT_ARROW, RIGHT_ARROW, TAB, UP_ARROW } from '@angular/cdk/keycodes';
import { ConnectionPositionPair, Overlay, OverlayConfig } from '@angular/cdk/overlay';
import { TemplatePortal } from '@angular/cdk/portal';
import { DOCUMENT } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, EventEmitter, Inject, Input, Optional, Output, TemplateRef, ViewChild, ViewContainerRef } from '@angular/core';
import { fromEvent, merge } from 'rxjs';
import { DEFAULT_MENTION_POSITIONS } from '../core/overlay/overlay-position';
import { InputBoolean } from '../core/util';
import { getMentions } from '../core/util/getMentions';
import { getCaretCoordinates } from '../core/util/textarea-caret-position';
import { NzMentionSuggestionDirective } from './nz-mention-suggestions';
import { NzMentionTriggerDirective } from './nz-mention-trigger';
/**
 * @record
 */
export function MentionOnSearchTypes() { }
if (false) {
    /** @type {?} */
    MentionOnSearchTypes.prototype.value;
    /** @type {?} */
    MentionOnSearchTypes.prototype.prefix;
}
/**
 * @record
 */
export function Mention() { }
if (false) {
    /** @type {?} */
    Mention.prototype.startPos;
    /** @type {?} */
    Mention.prototype.endPos;
    /** @type {?} */
    Mention.prototype.mention;
}
var NzMentionComponent = /** @class */ (function () {
    function NzMentionComponent(ngDocument, // tslint:disable-line:no-any
    changeDetectorRef, overlay, viewContainerRef) {
        this.ngDocument = ngDocument;
        this.changeDetectorRef = changeDetectorRef;
        this.overlay = overlay;
        this.viewContainerRef = viewContainerRef;
        this.nzValueWith = (/**
         * @param {?} value
         * @return {?}
         */
        function (value) { return value; }); // tslint:disable-line:no-any
        // tslint:disable-line:no-any
        this.nzPrefix = '@';
        this.nzLoading = false;
        this.nzNotFoundContent = '无匹配结果，轻敲空格完成输入';
        this.nzPlacement = 'bottom';
        this.nzSuggestions = [];
        this.nzOnSelect = new EventEmitter();
        this.nzOnSearchChange = new EventEmitter();
        this.isOpen = false;
        this.filteredSuggestions = [];
        this.suggestionTemplate = null; // tslint:disable-line:no-any
        // tslint:disable-line:no-any
        this.activeIndex = -1;
        this.previousValue = null;
    }
    Object.defineProperty(NzMentionComponent.prototype, "suggestionChild", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value) {
                this.suggestionTemplate = value;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzMentionComponent.prototype, "triggerNativeElement", {
        get: /**
         * @private
         * @return {?}
         */
        function () {
            return this.trigger.el.nativeElement;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?} changes
     * @return {?}
     */
    NzMentionComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.hasOwnProperty('nzSuggestions')) {
            if (this.isOpen) {
                this.previousValue = null;
                this.activeIndex = -1;
                this.resetDropdown(false);
            }
        }
    };
    /**
     * @return {?}
     */
    NzMentionComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        this.bindTriggerEvents();
    };
    /**
     * @return {?}
     */
    NzMentionComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.closeDropdown();
    };
    /**
     * @return {?}
     */
    NzMentionComponent.prototype.closeDropdown = /**
     * @return {?}
     */
    function () {
        if (this.overlayRef && this.overlayRef.hasAttached()) {
            this.overlayRef.detach();
            this.overlayBackdropClickSubscription.unsubscribe();
            this.isOpen = false;
            this.changeDetectorRef.markForCheck();
        }
    };
    /**
     * @return {?}
     */
    NzMentionComponent.prototype.openDropdown = /**
     * @return {?}
     */
    function () {
        this.attachOverlay();
        this.isOpen = true;
        this.changeDetectorRef.markForCheck();
    };
    /**
     * @return {?}
     */
    NzMentionComponent.prototype.getMentions = /**
     * @return {?}
     */
    function () {
        return getMentions(this.trigger.value, this.nzPrefix);
    };
    /**
     * @param {?} suggestion
     * @return {?}
     */
    NzMentionComponent.prototype.selectSuggestion = /**
     * @param {?} suggestion
     * @return {?}
     */
    function (suggestion) {
        /** @type {?} */
        var value = this.nzValueWith(suggestion);
        this.trigger.insertMention({
            mention: value,
            startPos: this.cursorMentionStart,
            endPos: this.cursorMentionEnd
        });
        this.nzOnSelect.emit(suggestion);
        this.closeDropdown();
        this.activeIndex = -1;
    };
    /**
     * @private
     * @param {?} event
     * @return {?}
     */
    NzMentionComponent.prototype.handleInput = /**
     * @private
     * @param {?} event
     * @return {?}
     */
    function (event) {
        /** @type {?} */
        var target = (/** @type {?} */ (event.target));
        this.trigger.onChange(target.value);
        this.trigger.value = target.value;
        this.resetDropdown();
    };
    /**
     * @private
     * @param {?} event
     * @return {?}
     */
    NzMentionComponent.prototype.handleKeydown = /**
     * @private
     * @param {?} event
     * @return {?}
     */
    function (event) {
        /** @type {?} */
        var keyCode = event.keyCode;
        if (this.isOpen && keyCode === ENTER && this.activeIndex !== -1 && this.filteredSuggestions.length) {
            this.selectSuggestion(this.filteredSuggestions[this.activeIndex]);
            event.preventDefault();
        }
        else if (keyCode === LEFT_ARROW || keyCode === RIGHT_ARROW) {
            this.resetDropdown();
            event.stopPropagation();
        }
        else {
            if (this.isOpen && (keyCode === TAB || keyCode === ESCAPE)) {
                this.closeDropdown();
                return;
            }
            if (this.isOpen && keyCode === UP_ARROW) {
                this.setPreviousItemActive();
                event.preventDefault();
                event.stopPropagation();
            }
            if (this.isOpen && keyCode === DOWN_ARROW) {
                this.setNextItemActive();
                event.preventDefault();
                event.stopPropagation();
            }
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.handleClick = /**
     * @private
     * @return {?}
     */
    function () {
        this.resetDropdown();
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.bindTriggerEvents = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        this.trigger.onInput.subscribe((/**
         * @param {?} e
         * @return {?}
         */
        function (e) { return _this.handleInput(e); }));
        this.trigger.onKeydown.subscribe((/**
         * @param {?} e
         * @return {?}
         */
        function (e) { return _this.handleKeydown(e); }));
        this.trigger.onClick.subscribe((/**
         * @return {?}
         */
        function () { return _this.handleClick(); }));
    };
    /**
     * @private
     * @param {?} value
     * @param {?} emit
     * @return {?}
     */
    NzMentionComponent.prototype.suggestionsFilter = /**
     * @private
     * @param {?} value
     * @param {?} emit
     * @return {?}
     */
    function (value, emit) {
        var _this = this;
        /** @type {?} */
        var suggestions = value.substring(1);
        if (this.previousValue === value) {
            return;
        }
        this.previousValue = value;
        if (emit) {
            this.nzOnSearchChange.emit({
                value: (/** @type {?} */ (this.cursorMention)).substring(1),
                prefix: (/** @type {?} */ (this.cursorMention))[0]
            });
        }
        /** @type {?} */
        var searchValue = suggestions.toLowerCase();
        this.filteredSuggestions = this.nzSuggestions.filter((/**
         * @param {?} suggestion
         * @return {?}
         */
        function (suggestion) {
            return _this.nzValueWith(suggestion)
                .toLowerCase()
                .includes(searchValue);
        }));
    };
    /**
     * @private
     * @param {?=} emit
     * @return {?}
     */
    NzMentionComponent.prototype.resetDropdown = /**
     * @private
     * @param {?=} emit
     * @return {?}
     */
    function (emit) {
        if (emit === void 0) { emit = true; }
        this.resetCursorMention();
        if (typeof this.cursorMention !== 'string' || !this.canOpen()) {
            this.closeDropdown();
            return;
        }
        this.suggestionsFilter(this.cursorMention, emit);
        /** @type {?} */
        var activeIndex = this.filteredSuggestions.indexOf(this.cursorMention.substring(1));
        this.activeIndex = activeIndex >= 0 ? activeIndex : 0;
        this.openDropdown();
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.setNextItemActive = /**
     * @private
     * @return {?}
     */
    function () {
        this.activeIndex = this.activeIndex + 1 <= this.filteredSuggestions.length - 1 ? this.activeIndex + 1 : 0;
        this.changeDetectorRef.markForCheck();
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.setPreviousItemActive = /**
     * @private
     * @return {?}
     */
    function () {
        this.activeIndex = this.activeIndex - 1 < 0 ? this.filteredSuggestions.length - 1 : this.activeIndex - 1;
        this.changeDetectorRef.markForCheck();
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.canOpen = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var element = this.triggerNativeElement;
        return !element.readOnly && !element.disabled;
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.resetCursorMention = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var value = this.triggerNativeElement.value.replace(/[\r\n]/g, ' ') || '';
        /** @type {?} */
        var selectionStart = (/** @type {?} */ (this.triggerNativeElement.selectionStart));
        /** @type {?} */
        var prefix = typeof this.nzPrefix === 'string' ? [this.nzPrefix] : this.nzPrefix;
        /** @type {?} */
        var i = prefix.length;
        while (i >= 0) {
            /** @type {?} */
            var startPos = value.lastIndexOf(prefix[i], selectionStart);
            /** @type {?} */
            var endPos = value.indexOf(' ', selectionStart) > -1 ? value.indexOf(' ', selectionStart) : value.length;
            /** @type {?} */
            var mention = value.substring(startPos, endPos);
            if ((startPos > 0 && value[startPos - 1] !== ' ') ||
                startPos < 0 ||
                mention.includes(prefix[i], 1) ||
                mention.includes(' ')) {
                this.cursorMention = null;
                this.cursorMentionStart = -1;
                this.cursorMentionEnd = -1;
            }
            else {
                this.cursorMention = mention;
                this.cursorMentionStart = startPos;
                this.cursorMentionEnd = endPos;
                return;
            }
            i--;
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.updatePositions = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var coordinates = getCaretCoordinates(this.triggerNativeElement, this.cursorMentionStart);
        /** @type {?} */
        var top = coordinates.top -
            this.triggerNativeElement.getBoundingClientRect().height -
            this.triggerNativeElement.scrollTop +
            (this.nzPlacement === 'bottom' ? coordinates.height : 0);
        /** @type {?} */
        var left = coordinates.left - this.triggerNativeElement.scrollLeft;
        this.positionStrategy.withDefaultOffsetX(left).withDefaultOffsetY(top);
        if (this.nzPlacement === 'bottom') {
            this.positionStrategy.withPositions([DEFAULT_MENTION_POSITIONS[0]]);
        }
        if (this.nzPlacement === 'top') {
            this.positionStrategy.withPositions([DEFAULT_MENTION_POSITIONS[1]]);
        }
        this.positionStrategy.apply();
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.subscribeOverlayBackdropClick = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        return merge(fromEvent(this.ngDocument, 'click'), fromEvent(this.ngDocument, 'touchend')).subscribe((/**
         * @param {?} event
         * @return {?}
         */
        function (event) {
            /** @type {?} */
            var clickTarget = (/** @type {?} */ (event.target));
            if (clickTarget !== _this.trigger.el.nativeElement && _this.isOpen) {
                _this.closeDropdown();
            }
        }));
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.attachOverlay = /**
     * @private
     * @return {?}
     */
    function () {
        if (!this.overlayRef) {
            this.portal = new TemplatePortal(this.suggestionsTemp, this.viewContainerRef);
            this.overlayRef = this.overlay.create(this.getOverlayConfig());
        }
        if (this.overlayRef && !this.overlayRef.hasAttached()) {
            this.overlayRef.attach(this.portal);
            this.overlayBackdropClickSubscription = this.subscribeOverlayBackdropClick();
        }
        this.updatePositions();
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.getOverlayConfig = /**
     * @private
     * @return {?}
     */
    function () {
        return new OverlayConfig({
            positionStrategy: this.getOverlayPosition(),
            scrollStrategy: this.overlay.scrollStrategies.reposition()
        });
    };
    /**
     * @private
     * @return {?}
     */
    NzMentionComponent.prototype.getOverlayPosition = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var positions = [
            new ConnectionPositionPair({ originX: 'start', originY: 'bottom' }, { overlayX: 'start', overlayY: 'top' }),
            new ConnectionPositionPair({ originX: 'start', originY: 'top' }, { overlayX: 'start', overlayY: 'bottom' })
        ];
        this.positionStrategy = this.overlay
            .position()
            .flexibleConnectedTo(this.trigger.el)
            .withPositions(positions)
            .withFlexibleDimensions(false)
            .withPush(false);
        return this.positionStrategy;
    };
    NzMentionComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-mention',
                    template: "<ng-content></ng-content>\n<ng-template #suggestions>\n  <ul class=\"ant-mention-dropdown\">\n    <li class=\"ant-mention-dropdown-item\"\n        *ngFor=\"let suggestion of filteredSuggestions; let i = index\"\n        [class.focus]=\"i === activeIndex\"\n        (mousedown)=\"$event.preventDefault()\"\n        (click)=\"selectSuggestion(suggestion)\">\n      <ng-container *ngIf=\"suggestionTemplate else defaultSuggestion\">\n        <ng-container *ngTemplateOutlet=\"suggestionTemplate; context: {$implicit: suggestion}\"></ng-container>\n      </ng-container>\n      <ng-template #defaultSuggestion>{{ nzValueWith(suggestion) }}</ng-template>\n    </li>\n    <li class=\"ant-mention-dropdown-notfound ant-mention-dropdown-item\"\n        *ngIf=\"filteredSuggestions.length === 0\">\n      <span *ngIf=\"nzLoading\"><i nz-icon type=\"loading\"></i></span>\n      <span *ngIf=\"!nzLoading\">{{ nzNotFoundContent }}</span>\n    </li>\n  </ul>\n</ng-template>\n",
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    styles: ["\n      .ant-mention-dropdown {\n        top: 100%;\n        left: 0;\n        position: relative;\n        width: 100%;\n        margin-top: 4px;\n        margin-bottom: 4px;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzMentionComponent.ctorParameters = function () { return [
        { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [DOCUMENT,] }] },
        { type: ChangeDetectorRef },
        { type: Overlay },
        { type: ViewContainerRef }
    ]; };
    NzMentionComponent.propDecorators = {
        nzValueWith: [{ type: Input }],
        nzPrefix: [{ type: Input }],
        nzLoading: [{ type: Input }],
        nzNotFoundContent: [{ type: Input }],
        nzPlacement: [{ type: Input }],
        nzSuggestions: [{ type: Input }],
        nzOnSelect: [{ type: Output }],
        nzOnSearchChange: [{ type: Output }],
        trigger: [{ type: ContentChild, args: [NzMentionTriggerDirective,] }],
        suggestionsTemp: [{ type: ViewChild, args: [TemplateRef,] }],
        suggestionChild: [{ type: ContentChild, args: [NzMentionSuggestionDirective, { read: TemplateRef },] }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzMentionComponent.prototype, "nzLoading", void 0);
    return NzMentionComponent;
}());
export { NzMentionComponent };
if (false) {
    /** @type {?} */
    NzMentionComponent.prototype.nzValueWith;
    /** @type {?} */
    NzMentionComponent.prototype.nzPrefix;
    /** @type {?} */
    NzMentionComponent.prototype.nzLoading;
    /** @type {?} */
    NzMentionComponent.prototype.nzNotFoundContent;
    /** @type {?} */
    NzMentionComponent.prototype.nzPlacement;
    /** @type {?} */
    NzMentionComponent.prototype.nzSuggestions;
    /** @type {?} */
    NzMentionComponent.prototype.nzOnSelect;
    /** @type {?} */
    NzMentionComponent.prototype.nzOnSearchChange;
    /** @type {?} */
    NzMentionComponent.prototype.trigger;
    /** @type {?} */
    NzMentionComponent.prototype.suggestionsTemp;
    /** @type {?} */
    NzMentionComponent.prototype.isOpen;
    /** @type {?} */
    NzMentionComponent.prototype.filteredSuggestions;
    /** @type {?} */
    NzMentionComponent.prototype.suggestionTemplate;
    /** @type {?} */
    NzMentionComponent.prototype.activeIndex;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.previousValue;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.cursorMention;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.cursorMentionStart;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.cursorMentionEnd;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.overlayRef;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.portal;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.positionStrategy;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.overlayBackdropClickSubscription;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.ngDocument;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.changeDetectorRef;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.overlay;
    /**
     * @type {?}
     * @private
     */
    NzMentionComponent.prototype.viewContainerRef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVudGlvbi5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibWVudGlvbi9uei1tZW50aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxVQUFVLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLEdBQUcsRUFBRSxRQUFRLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUMxRyxPQUFPLEVBQ0wsc0JBQXNCLEVBRXRCLE9BQU8sRUFDUCxhQUFhLEVBR2QsTUFBTSxzQkFBc0IsQ0FBQztBQUM5QixPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFFckQsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQzNDLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osWUFBWSxFQUNaLE1BQU0sRUFDTixLQUFLLEVBR0wsUUFBUSxFQUNSLE1BQU0sRUFFTixXQUFXLEVBQ1gsU0FBUyxFQUNULGdCQUFnQixFQUNqQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBZ0IsTUFBTSxNQUFNLENBQUM7QUFFdEQsT0FBTyxFQUFFLHlCQUF5QixFQUFFLE1BQU0sa0NBQWtDLENBQUM7QUFDN0UsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGNBQWMsQ0FBQztBQUM1QyxPQUFPLEVBQUUsV0FBVyxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDdkQsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sc0NBQXNDLENBQUM7QUFFM0UsT0FBTyxFQUFFLDRCQUE0QixFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDeEUsT0FBTyxFQUFFLHlCQUF5QixFQUFFLE1BQU0sc0JBQXNCLENBQUM7Ozs7QUFFakUsMENBR0M7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QkFDMUMsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07NkJBRTdDLGdNQVNDO2lCQUVKOzs7O2dEQXlDSSxRQUFRLFlBQUksTUFBTSxTQUFDLFFBQVE7Z0JBaEc5QixpQkFBaUI7Z0JBWGpCLE9BQU87Z0JBd0JQLGdCQUFnQjs7OzhCQTRDZixLQUFLOzJCQUNMLEtBQUs7NEJBQ0wsS0FBSztvQ0FDTCxLQUFLOzhCQUNMLEtBQUs7Z0NBQ0wsS0FBSzs2QkFDTCxNQUFNO21DQUNOLE1BQU07MEJBRU4sWUFBWSxTQUFDLHlCQUF5QjtrQ0FDdEMsU0FBUyxTQUFDLFdBQVc7a0NBRXJCLFlBQVksU0FBQyw0QkFBNEIsRUFBRSxFQUFFLElBQUksRUFBRSxXQUFXLEVBQUU7O0lBVnhDO1FBQWYsWUFBWSxFQUFFOzt5REFBbUI7SUFrUjdDLHlCQUFDO0NBQUEsQUF2U0QsSUF1U0M7U0FyUlksa0JBQWtCOzs7SUFDN0IseUNBQThEOztJQUM5RCxzQ0FBMkM7O0lBQzNDLHVDQUEyQzs7SUFDM0MsK0NBQXNEOztJQUN0RCx5Q0FBa0Q7O0lBQ2xELDJDQUFzQzs7SUFDdEMsd0NBQThFOztJQUM5RSw4Q0FBNkY7O0lBRTdGLHFDQUE0RTs7SUFDNUUsNkNBQTJEOztJQVUzRCxvQ0FBZTs7SUFDZixpREFBbUM7O0lBQ25DLGdEQUFrRTs7SUFDbEUseUNBQWlCOzs7OztJQUVqQiwyQ0FBNEM7Ozs7O0lBQzVDLDJDQUFxQzs7Ozs7SUFDckMsZ0RBQW1DOzs7OztJQUNuQyw4Q0FBaUM7Ozs7O0lBQ2pDLHdDQUFzQzs7Ozs7SUFDdEMsb0NBQXFDOzs7OztJQUNyQyw4Q0FBNEQ7Ozs7O0lBQzVELDhEQUF1RDs7Ozs7SUFPckQsd0NBQXFEOzs7OztJQUNyRCwrQ0FBNEM7Ozs7O0lBQzVDLHFDQUF3Qjs7Ozs7SUFDeEIsOENBQTBDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRE9XTl9BUlJPVywgRU5URVIsIEVTQ0FQRSwgTEVGVF9BUlJPVywgUklHSFRfQVJST1csIFRBQiwgVVBfQVJST1cgfSBmcm9tICdAYW5ndWxhci9jZGsva2V5Y29kZXMnO1xuaW1wb3J0IHtcbiAgQ29ubmVjdGlvblBvc2l0aW9uUGFpcixcbiAgRmxleGlibGVDb25uZWN0ZWRQb3NpdGlvblN0cmF0ZWd5LFxuICBPdmVybGF5LFxuICBPdmVybGF5Q29uZmlnLFxuICBPdmVybGF5UmVmLFxuICBQb3NpdGlvblN0cmF0ZWd5XG59IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IFRlbXBsYXRlUG9ydGFsIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL3BvcnRhbCc7XG5cbmltcG9ydCB7IERPQ1VNRU5UIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7XG4gIEFmdGVyQ29udGVudEluaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5qZWN0LFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdDb250YWluZXJSZWZcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBmcm9tRXZlbnQsIG1lcmdlLCBTdWJzY3JpcHRpb24gfSBmcm9tICdyeGpzJztcblxuaW1wb3J0IHsgREVGQVVMVF9NRU5USU9OX1BPU0lUSU9OUyB9IGZyb20gJy4uL2NvcmUvb3ZlcmxheS9vdmVybGF5LXBvc2l0aW9uJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbCc7XG5pbXBvcnQgeyBnZXRNZW50aW9ucyB9IGZyb20gJy4uL2NvcmUvdXRpbC9nZXRNZW50aW9ucyc7XG5pbXBvcnQgeyBnZXRDYXJldENvb3JkaW5hdGVzIH0gZnJvbSAnLi4vY29yZS91dGlsL3RleHRhcmVhLWNhcmV0LXBvc2l0aW9uJztcblxuaW1wb3J0IHsgTnpNZW50aW9uU3VnZ2VzdGlvbkRpcmVjdGl2ZSB9IGZyb20gJy4vbnotbWVudGlvbi1zdWdnZXN0aW9ucyc7XG5pbXBvcnQgeyBOek1lbnRpb25UcmlnZ2VyRGlyZWN0aXZlIH0gZnJvbSAnLi9uei1tZW50aW9uLXRyaWdnZXInO1xuXG5leHBvcnQgaW50ZXJmYWNlIE1lbnRpb25PblNlYXJjaFR5cGVzIHtcbiAgdmFsdWU6IHN0cmluZztcbiAgcHJlZml4OiBzdHJpbmc7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgTWVudGlvbiB7XG4gIHN0YXJ0UG9zOiBudW1iZXI7XG4gIGVuZFBvczogbnVtYmVyO1xuICBtZW50aW9uOiBzdHJpbmc7XG59XG5cbmV4cG9ydCB0eXBlIE1lbnRpb25QbGFjZW1lbnQgPSAndG9wJyB8ICdib3R0b20nO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1tZW50aW9uJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LW1lbnRpb24uY29tcG9uZW50Lmh0bWwnLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHN0eWxlczogW1xuICAgIGBcbiAgICAgIC5hbnQtbWVudGlvbi1kcm9wZG93biB7XG4gICAgICAgIHRvcDogMTAwJTtcbiAgICAgICAgbGVmdDogMDtcbiAgICAgICAgcG9zaXRpb246IHJlbGF0aXZlO1xuICAgICAgICB3aWR0aDogMTAwJTtcbiAgICAgICAgbWFyZ2luLXRvcDogNHB4O1xuICAgICAgICBtYXJnaW4tYm90dG9tOiA0cHg7XG4gICAgICB9XG4gICAgYFxuICBdXG59KVxuZXhwb3J0IGNsYXNzIE56TWVudGlvbkNvbXBvbmVudCBpbXBsZW1lbnRzIE9uRGVzdHJveSwgQWZ0ZXJDb250ZW50SW5pdCwgT25DaGFuZ2VzIHtcbiAgQElucHV0KCkgbnpWYWx1ZVdpdGg6ICh2YWx1ZTogYW55KSA9PiBzdHJpbmcgPSB2YWx1ZSA9PiB2YWx1ZTsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcbiAgQElucHV0KCkgbnpQcmVmaXg6IHN0cmluZyB8IHN0cmluZ1tdID0gJ0AnO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpMb2FkaW5nID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56Tm90Rm91bmRDb250ZW50OiBzdHJpbmcgPSAn5peg5Yy56YWN57uT5p6c77yM6L275pWy56m65qC85a6M5oiQ6L6T5YWlJztcbiAgQElucHV0KCkgbnpQbGFjZW1lbnQ6IE1lbnRpb25QbGFjZW1lbnQgPSAnYm90dG9tJztcbiAgQElucHV0KCkgbnpTdWdnZXN0aW9uczogc3RyaW5nW10gPSBbXTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25TZWxlY3Q6IEV2ZW50RW1pdHRlcjxzdHJpbmcgfCB7fT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uU2VhcmNoQ2hhbmdlOiBFdmVudEVtaXR0ZXI8TWVudGlvbk9uU2VhcmNoVHlwZXM+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIEBDb250ZW50Q2hpbGQoTnpNZW50aW9uVHJpZ2dlckRpcmVjdGl2ZSkgdHJpZ2dlcjogTnpNZW50aW9uVHJpZ2dlckRpcmVjdGl2ZTtcbiAgQFZpZXdDaGlsZChUZW1wbGF0ZVJlZikgc3VnZ2VzdGlvbnNUZW1wOiBUZW1wbGF0ZVJlZjx2b2lkPjtcblxuICBAQ29udGVudENoaWxkKE56TWVudGlvblN1Z2dlc3Rpb25EaXJlY3RpdmUsIHsgcmVhZDogVGVtcGxhdGVSZWYgfSlcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBzZXQgc3VnZ2VzdGlvbkNoaWxkKHZhbHVlOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogYW55IH0+KSB7XG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLnN1Z2dlc3Rpb25UZW1wbGF0ZSA9IHZhbHVlO1xuICAgIH1cbiAgfVxuXG4gIGlzT3BlbiA9IGZhbHNlO1xuICBmaWx0ZXJlZFN1Z2dlc3Rpb25zOiBzdHJpbmdbXSA9IFtdO1xuICBzdWdnZXN0aW9uVGVtcGxhdGU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBhbnkgfT4gfCBudWxsID0gbnVsbDsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcbiAgYWN0aXZlSW5kZXggPSAtMTtcblxuICBwcml2YXRlIHByZXZpb3VzVmFsdWU6IHN0cmluZyB8IG51bGwgPSBudWxsO1xuICBwcml2YXRlIGN1cnNvck1lbnRpb246IHN0cmluZyB8IG51bGw7XG4gIHByaXZhdGUgY3Vyc29yTWVudGlvblN0YXJ0OiBudW1iZXI7XG4gIHByaXZhdGUgY3Vyc29yTWVudGlvbkVuZDogbnVtYmVyO1xuICBwcml2YXRlIG92ZXJsYXlSZWY6IE92ZXJsYXlSZWYgfCBudWxsO1xuICBwcml2YXRlIHBvcnRhbDogVGVtcGxhdGVQb3J0YWw8dm9pZD47XG4gIHByaXZhdGUgcG9zaXRpb25TdHJhdGVneTogRmxleGlibGVDb25uZWN0ZWRQb3NpdGlvblN0cmF0ZWd5O1xuICBwcml2YXRlIG92ZXJsYXlCYWNrZHJvcENsaWNrU3Vic2NyaXB0aW9uOiBTdWJzY3JpcHRpb247XG5cbiAgcHJpdmF0ZSBnZXQgdHJpZ2dlck5hdGl2ZUVsZW1lbnQoKTogSFRNTFRleHRBcmVhRWxlbWVudCB8IEhUTUxJbnB1dEVsZW1lbnQge1xuICAgIHJldHVybiB0aGlzLnRyaWdnZXIuZWwubmF0aXZlRWxlbWVudDtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIEBPcHRpb25hbCgpIEBJbmplY3QoRE9DVU1FTlQpIHByaXZhdGUgbmdEb2N1bWVudDogYW55LCAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuICAgIHByaXZhdGUgY2hhbmdlRGV0ZWN0b3JSZWY6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByaXZhdGUgb3ZlcmxheTogT3ZlcmxheSxcbiAgICBwcml2YXRlIHZpZXdDb250YWluZXJSZWY6IFZpZXdDb250YWluZXJSZWZcbiAgKSB7fVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5oYXNPd25Qcm9wZXJ0eSgnbnpTdWdnZXN0aW9ucycpKSB7XG4gICAgICBpZiAodGhpcy5pc09wZW4pIHtcbiAgICAgICAgdGhpcy5wcmV2aW91c1ZhbHVlID0gbnVsbDtcbiAgICAgICAgdGhpcy5hY3RpdmVJbmRleCA9IC0xO1xuICAgICAgICB0aGlzLnJlc2V0RHJvcGRvd24oZmFsc2UpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIG5nQWZ0ZXJDb250ZW50SW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmJpbmRUcmlnZ2VyRXZlbnRzKCk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmNsb3NlRHJvcGRvd24oKTtcbiAgfVxuXG4gIGNsb3NlRHJvcGRvd24oKTogdm9pZCB7XG4gICAgaWYgKHRoaXMub3ZlcmxheVJlZiAmJiB0aGlzLm92ZXJsYXlSZWYuaGFzQXR0YWNoZWQoKSkge1xuICAgICAgdGhpcy5vdmVybGF5UmVmLmRldGFjaCgpO1xuICAgICAgdGhpcy5vdmVybGF5QmFja2Ryb3BDbGlja1N1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpO1xuICAgICAgdGhpcy5pc09wZW4gPSBmYWxzZTtcbiAgICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYubWFya0ZvckNoZWNrKCk7XG4gICAgfVxuICB9XG5cbiAgb3BlbkRyb3Bkb3duKCk6IHZvaWQge1xuICAgIHRoaXMuYXR0YWNoT3ZlcmxheSgpO1xuICAgIHRoaXMuaXNPcGVuID0gdHJ1ZTtcbiAgICB0aGlzLmNoYW5nZURldGVjdG9yUmVmLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgZ2V0TWVudGlvbnMoKTogc3RyaW5nW10ge1xuICAgIHJldHVybiBnZXRNZW50aW9ucyh0aGlzLnRyaWdnZXIudmFsdWUsIHRoaXMubnpQcmVmaXgpO1xuICB9XG5cbiAgc2VsZWN0U3VnZ2VzdGlvbihzdWdnZXN0aW9uOiBzdHJpbmcgfCB7fSk6IHZvaWQge1xuICAgIGNvbnN0IHZhbHVlID0gdGhpcy5uelZhbHVlV2l0aChzdWdnZXN0aW9uKTtcbiAgICB0aGlzLnRyaWdnZXIuaW5zZXJ0TWVudGlvbih7XG4gICAgICBtZW50aW9uOiB2YWx1ZSxcbiAgICAgIHN0YXJ0UG9zOiB0aGlzLmN1cnNvck1lbnRpb25TdGFydCxcbiAgICAgIGVuZFBvczogdGhpcy5jdXJzb3JNZW50aW9uRW5kXG4gICAgfSk7XG4gICAgdGhpcy5uek9uU2VsZWN0LmVtaXQoc3VnZ2VzdGlvbik7XG4gICAgdGhpcy5jbG9zZURyb3Bkb3duKCk7XG4gICAgdGhpcy5hY3RpdmVJbmRleCA9IC0xO1xuICB9XG5cbiAgcHJpdmF0ZSBoYW5kbGVJbnB1dChldmVudDogS2V5Ym9hcmRFdmVudCk6IHZvaWQge1xuICAgIGNvbnN0IHRhcmdldCA9IGV2ZW50LnRhcmdldCBhcyBIVE1MSW5wdXRFbGVtZW50IHwgSFRNTFRleHRBcmVhRWxlbWVudDtcbiAgICB0aGlzLnRyaWdnZXIub25DaGFuZ2UodGFyZ2V0LnZhbHVlKTtcbiAgICB0aGlzLnRyaWdnZXIudmFsdWUgPSB0YXJnZXQudmFsdWU7XG4gICAgdGhpcy5yZXNldERyb3Bkb3duKCk7XG4gIH1cblxuICBwcml2YXRlIGhhbmRsZUtleWRvd24oZXZlbnQ6IEtleWJvYXJkRXZlbnQpOiB2b2lkIHtcbiAgICBjb25zdCBrZXlDb2RlID0gZXZlbnQua2V5Q29kZTtcbiAgICBpZiAodGhpcy5pc09wZW4gJiYga2V5Q29kZSA9PT0gRU5URVIgJiYgdGhpcy5hY3RpdmVJbmRleCAhPT0gLTEgJiYgdGhpcy5maWx0ZXJlZFN1Z2dlc3Rpb25zLmxlbmd0aCkge1xuICAgICAgdGhpcy5zZWxlY3RTdWdnZXN0aW9uKHRoaXMuZmlsdGVyZWRTdWdnZXN0aW9uc1t0aGlzLmFjdGl2ZUluZGV4XSk7XG4gICAgICBldmVudC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIH0gZWxzZSBpZiAoa2V5Q29kZSA9PT0gTEVGVF9BUlJPVyB8fCBrZXlDb2RlID09PSBSSUdIVF9BUlJPVykge1xuICAgICAgdGhpcy5yZXNldERyb3Bkb3duKCk7XG4gICAgICBldmVudC5zdG9wUHJvcGFnYXRpb24oKTtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHRoaXMuaXNPcGVuICYmIChrZXlDb2RlID09PSBUQUIgfHwga2V5Q29kZSA9PT0gRVNDQVBFKSkge1xuICAgICAgICB0aGlzLmNsb3NlRHJvcGRvd24oKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICBpZiAodGhpcy5pc09wZW4gJiYga2V5Q29kZSA9PT0gVVBfQVJST1cpIHtcbiAgICAgICAgdGhpcy5zZXRQcmV2aW91c0l0ZW1BY3RpdmUoKTtcbiAgICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgICB9XG5cbiAgICAgIGlmICh0aGlzLmlzT3BlbiAmJiBrZXlDb2RlID09PSBET1dOX0FSUk9XKSB7XG4gICAgICAgIHRoaXMuc2V0TmV4dEl0ZW1BY3RpdmUoKTtcbiAgICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBoYW5kbGVDbGljaygpOiB2b2lkIHtcbiAgICB0aGlzLnJlc2V0RHJvcGRvd24oKTtcbiAgfVxuXG4gIHByaXZhdGUgYmluZFRyaWdnZXJFdmVudHMoKTogdm9pZCB7XG4gICAgdGhpcy50cmlnZ2VyLm9uSW5wdXQuc3Vic2NyaWJlKChlOiBLZXlib2FyZEV2ZW50KSA9PiB0aGlzLmhhbmRsZUlucHV0KGUpKTtcbiAgICB0aGlzLnRyaWdnZXIub25LZXlkb3duLnN1YnNjcmliZSgoZTogS2V5Ym9hcmRFdmVudCkgPT4gdGhpcy5oYW5kbGVLZXlkb3duKGUpKTtcbiAgICB0aGlzLnRyaWdnZXIub25DbGljay5zdWJzY3JpYmUoKCkgPT4gdGhpcy5oYW5kbGVDbGljaygpKTtcbiAgfVxuXG4gIHByaXZhdGUgc3VnZ2VzdGlvbnNGaWx0ZXIodmFsdWU6IHN0cmluZywgZW1pdDogYm9vbGVhbik6IHZvaWQge1xuICAgIGNvbnN0IHN1Z2dlc3Rpb25zID0gdmFsdWUuc3Vic3RyaW5nKDEpO1xuICAgIGlmICh0aGlzLnByZXZpb3VzVmFsdWUgPT09IHZhbHVlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMucHJldmlvdXNWYWx1ZSA9IHZhbHVlO1xuICAgIGlmIChlbWl0KSB7XG4gICAgICB0aGlzLm56T25TZWFyY2hDaGFuZ2UuZW1pdCh7XG4gICAgICAgIHZhbHVlOiB0aGlzLmN1cnNvck1lbnRpb24hLnN1YnN0cmluZygxKSxcbiAgICAgICAgcHJlZml4OiB0aGlzLmN1cnNvck1lbnRpb24hWzBdXG4gICAgICB9KTtcbiAgICB9XG4gICAgY29uc3Qgc2VhcmNoVmFsdWUgPSBzdWdnZXN0aW9ucy50b0xvd2VyQ2FzZSgpO1xuICAgIHRoaXMuZmlsdGVyZWRTdWdnZXN0aW9ucyA9IHRoaXMubnpTdWdnZXN0aW9ucy5maWx0ZXIoc3VnZ2VzdGlvbiA9PlxuICAgICAgdGhpcy5uelZhbHVlV2l0aChzdWdnZXN0aW9uKVxuICAgICAgICAudG9Mb3dlckNhc2UoKVxuICAgICAgICAuaW5jbHVkZXMoc2VhcmNoVmFsdWUpXG4gICAgKTtcbiAgfVxuXG4gIHByaXZhdGUgcmVzZXREcm9wZG93bihlbWl0OiBib29sZWFuID0gdHJ1ZSk6IHZvaWQge1xuICAgIHRoaXMucmVzZXRDdXJzb3JNZW50aW9uKCk7XG4gICAgaWYgKHR5cGVvZiB0aGlzLmN1cnNvck1lbnRpb24gIT09ICdzdHJpbmcnIHx8ICF0aGlzLmNhbk9wZW4oKSkge1xuICAgICAgdGhpcy5jbG9zZURyb3Bkb3duKCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuc3VnZ2VzdGlvbnNGaWx0ZXIodGhpcy5jdXJzb3JNZW50aW9uLCBlbWl0KTtcbiAgICBjb25zdCBhY3RpdmVJbmRleCA9IHRoaXMuZmlsdGVyZWRTdWdnZXN0aW9ucy5pbmRleE9mKHRoaXMuY3Vyc29yTWVudGlvbi5zdWJzdHJpbmcoMSkpO1xuICAgIHRoaXMuYWN0aXZlSW5kZXggPSBhY3RpdmVJbmRleCA+PSAwID8gYWN0aXZlSW5kZXggOiAwO1xuICAgIHRoaXMub3BlbkRyb3Bkb3duKCk7XG4gIH1cblxuICBwcml2YXRlIHNldE5leHRJdGVtQWN0aXZlKCk6IHZvaWQge1xuICAgIHRoaXMuYWN0aXZlSW5kZXggPSB0aGlzLmFjdGl2ZUluZGV4ICsgMSA8PSB0aGlzLmZpbHRlcmVkU3VnZ2VzdGlvbnMubGVuZ3RoIC0gMSA/IHRoaXMuYWN0aXZlSW5kZXggKyAxIDogMDtcbiAgICB0aGlzLmNoYW5nZURldGVjdG9yUmVmLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgcHJpdmF0ZSBzZXRQcmV2aW91c0l0ZW1BY3RpdmUoKTogdm9pZCB7XG4gICAgdGhpcy5hY3RpdmVJbmRleCA9IHRoaXMuYWN0aXZlSW5kZXggLSAxIDwgMCA/IHRoaXMuZmlsdGVyZWRTdWdnZXN0aW9ucy5sZW5ndGggLSAxIDogdGhpcy5hY3RpdmVJbmRleCAtIDE7XG4gICAgdGhpcy5jaGFuZ2VEZXRlY3RvclJlZi5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHByaXZhdGUgY2FuT3BlbigpOiBib29sZWFuIHtcbiAgICBjb25zdCBlbGVtZW50OiBIVE1MSW5wdXRFbGVtZW50IHwgSFRNTFRleHRBcmVhRWxlbWVudCA9IHRoaXMudHJpZ2dlck5hdGl2ZUVsZW1lbnQ7XG4gICAgcmV0dXJuICFlbGVtZW50LnJlYWRPbmx5ICYmICFlbGVtZW50LmRpc2FibGVkO1xuICB9XG5cbiAgcHJpdmF0ZSByZXNldEN1cnNvck1lbnRpb24oKTogdm9pZCB7XG4gICAgY29uc3QgdmFsdWUgPSB0aGlzLnRyaWdnZXJOYXRpdmVFbGVtZW50LnZhbHVlLnJlcGxhY2UoL1tcXHJcXG5dL2csICcgJykgfHwgJyc7XG4gICAgY29uc3Qgc2VsZWN0aW9uU3RhcnQgPSB0aGlzLnRyaWdnZXJOYXRpdmVFbGVtZW50LnNlbGVjdGlvblN0YXJ0ITtcbiAgICBjb25zdCBwcmVmaXggPSB0eXBlb2YgdGhpcy5uelByZWZpeCA9PT0gJ3N0cmluZycgPyBbdGhpcy5uelByZWZpeF0gOiB0aGlzLm56UHJlZml4O1xuICAgIGxldCBpID0gcHJlZml4Lmxlbmd0aDtcbiAgICB3aGlsZSAoaSA+PSAwKSB7XG4gICAgICBjb25zdCBzdGFydFBvcyA9IHZhbHVlLmxhc3RJbmRleE9mKHByZWZpeFtpXSwgc2VsZWN0aW9uU3RhcnQpO1xuICAgICAgY29uc3QgZW5kUG9zID0gdmFsdWUuaW5kZXhPZignICcsIHNlbGVjdGlvblN0YXJ0KSA+IC0xID8gdmFsdWUuaW5kZXhPZignICcsIHNlbGVjdGlvblN0YXJ0KSA6IHZhbHVlLmxlbmd0aDtcbiAgICAgIGNvbnN0IG1lbnRpb24gPSB2YWx1ZS5zdWJzdHJpbmcoc3RhcnRQb3MsIGVuZFBvcyk7XG4gICAgICBpZiAoXG4gICAgICAgIChzdGFydFBvcyA+IDAgJiYgdmFsdWVbc3RhcnRQb3MgLSAxXSAhPT0gJyAnKSB8fFxuICAgICAgICBzdGFydFBvcyA8IDAgfHxcbiAgICAgICAgbWVudGlvbi5pbmNsdWRlcyhwcmVmaXhbaV0sIDEpIHx8XG4gICAgICAgIG1lbnRpb24uaW5jbHVkZXMoJyAnKVxuICAgICAgKSB7XG4gICAgICAgIHRoaXMuY3Vyc29yTWVudGlvbiA9IG51bGw7XG4gICAgICAgIHRoaXMuY3Vyc29yTWVudGlvblN0YXJ0ID0gLTE7XG4gICAgICAgIHRoaXMuY3Vyc29yTWVudGlvbkVuZCA9IC0xO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5jdXJzb3JNZW50aW9uID0gbWVudGlvbjtcbiAgICAgICAgdGhpcy5jdXJzb3JNZW50aW9uU3RhcnQgPSBzdGFydFBvcztcbiAgICAgICAgdGhpcy5jdXJzb3JNZW50aW9uRW5kID0gZW5kUG9zO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgICBpLS07XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSB1cGRhdGVQb3NpdGlvbnMoKTogdm9pZCB7XG4gICAgY29uc3QgY29vcmRpbmF0ZXMgPSBnZXRDYXJldENvb3JkaW5hdGVzKHRoaXMudHJpZ2dlck5hdGl2ZUVsZW1lbnQsIHRoaXMuY3Vyc29yTWVudGlvblN0YXJ0KTtcbiAgICBjb25zdCB0b3AgPVxuICAgICAgY29vcmRpbmF0ZXMudG9wIC1cbiAgICAgIHRoaXMudHJpZ2dlck5hdGl2ZUVsZW1lbnQuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCkuaGVpZ2h0IC1cbiAgICAgIHRoaXMudHJpZ2dlck5hdGl2ZUVsZW1lbnQuc2Nyb2xsVG9wICtcbiAgICAgICh0aGlzLm56UGxhY2VtZW50ID09PSAnYm90dG9tJyA/IGNvb3JkaW5hdGVzLmhlaWdodCA6IDApO1xuICAgIGNvbnN0IGxlZnQgPSBjb29yZGluYXRlcy5sZWZ0IC0gdGhpcy50cmlnZ2VyTmF0aXZlRWxlbWVudC5zY3JvbGxMZWZ0O1xuICAgIHRoaXMucG9zaXRpb25TdHJhdGVneS53aXRoRGVmYXVsdE9mZnNldFgobGVmdCkud2l0aERlZmF1bHRPZmZzZXRZKHRvcCk7XG4gICAgaWYgKHRoaXMubnpQbGFjZW1lbnQgPT09ICdib3R0b20nKSB7XG4gICAgICB0aGlzLnBvc2l0aW9uU3RyYXRlZ3kud2l0aFBvc2l0aW9ucyhbREVGQVVMVF9NRU5USU9OX1BPU0lUSU9OU1swXV0pO1xuICAgIH1cbiAgICBpZiAodGhpcy5uelBsYWNlbWVudCA9PT0gJ3RvcCcpIHtcbiAgICAgIHRoaXMucG9zaXRpb25TdHJhdGVneS53aXRoUG9zaXRpb25zKFtERUZBVUxUX01FTlRJT05fUE9TSVRJT05TWzFdXSk7XG4gICAgfVxuICAgIHRoaXMucG9zaXRpb25TdHJhdGVneS5hcHBseSgpO1xuICB9XG5cbiAgcHJpdmF0ZSBzdWJzY3JpYmVPdmVybGF5QmFja2Ryb3BDbGljaygpOiBTdWJzY3JpcHRpb24ge1xuICAgIHJldHVybiBtZXJnZTxNb3VzZUV2ZW50IHwgVG91Y2hFdmVudD4oXG4gICAgICBmcm9tRXZlbnQ8TW91c2VFdmVudD4odGhpcy5uZ0RvY3VtZW50LCAnY2xpY2snKSxcbiAgICAgIGZyb21FdmVudDxUb3VjaEV2ZW50Pih0aGlzLm5nRG9jdW1lbnQsICd0b3VjaGVuZCcpXG4gICAgKS5zdWJzY3JpYmUoKGV2ZW50OiBNb3VzZUV2ZW50IHwgVG91Y2hFdmVudCkgPT4ge1xuICAgICAgY29uc3QgY2xpY2tUYXJnZXQgPSBldmVudC50YXJnZXQgYXMgSFRNTEVsZW1lbnQ7XG4gICAgICBpZiAoY2xpY2tUYXJnZXQgIT09IHRoaXMudHJpZ2dlci5lbC5uYXRpdmVFbGVtZW50ICYmIHRoaXMuaXNPcGVuKSB7XG4gICAgICAgIHRoaXMuY2xvc2VEcm9wZG93bigpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBhdHRhY2hPdmVybGF5KCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5vdmVybGF5UmVmKSB7XG4gICAgICB0aGlzLnBvcnRhbCA9IG5ldyBUZW1wbGF0ZVBvcnRhbCh0aGlzLnN1Z2dlc3Rpb25zVGVtcCwgdGhpcy52aWV3Q29udGFpbmVyUmVmKTtcbiAgICAgIHRoaXMub3ZlcmxheVJlZiA9IHRoaXMub3ZlcmxheS5jcmVhdGUodGhpcy5nZXRPdmVybGF5Q29uZmlnKCkpO1xuICAgIH1cbiAgICBpZiAodGhpcy5vdmVybGF5UmVmICYmICF0aGlzLm92ZXJsYXlSZWYuaGFzQXR0YWNoZWQoKSkge1xuICAgICAgdGhpcy5vdmVybGF5UmVmLmF0dGFjaCh0aGlzLnBvcnRhbCk7XG4gICAgICB0aGlzLm92ZXJsYXlCYWNrZHJvcENsaWNrU3Vic2NyaXB0aW9uID0gdGhpcy5zdWJzY3JpYmVPdmVybGF5QmFja2Ryb3BDbGljaygpO1xuICAgIH1cbiAgICB0aGlzLnVwZGF0ZVBvc2l0aW9ucygpO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRPdmVybGF5Q29uZmlnKCk6IE92ZXJsYXlDb25maWcge1xuICAgIHJldHVybiBuZXcgT3ZlcmxheUNvbmZpZyh7XG4gICAgICBwb3NpdGlvblN0cmF0ZWd5OiB0aGlzLmdldE92ZXJsYXlQb3NpdGlvbigpLFxuICAgICAgc2Nyb2xsU3RyYXRlZ3k6IHRoaXMub3ZlcmxheS5zY3JvbGxTdHJhdGVnaWVzLnJlcG9zaXRpb24oKVxuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRPdmVybGF5UG9zaXRpb24oKTogUG9zaXRpb25TdHJhdGVneSB7XG4gICAgY29uc3QgcG9zaXRpb25zID0gW1xuICAgICAgbmV3IENvbm5lY3Rpb25Qb3NpdGlvblBhaXIoeyBvcmlnaW5YOiAnc3RhcnQnLCBvcmlnaW5ZOiAnYm90dG9tJyB9LCB7IG92ZXJsYXlYOiAnc3RhcnQnLCBvdmVybGF5WTogJ3RvcCcgfSksXG4gICAgICBuZXcgQ29ubmVjdGlvblBvc2l0aW9uUGFpcih7IG9yaWdpblg6ICdzdGFydCcsIG9yaWdpblk6ICd0b3AnIH0sIHsgb3ZlcmxheVg6ICdzdGFydCcsIG92ZXJsYXlZOiAnYm90dG9tJyB9KVxuICAgIF07XG4gICAgdGhpcy5wb3NpdGlvblN0cmF0ZWd5ID0gdGhpcy5vdmVybGF5XG4gICAgICAucG9zaXRpb24oKVxuICAgICAgLmZsZXhpYmxlQ29ubmVjdGVkVG8odGhpcy50cmlnZ2VyLmVsKVxuICAgICAgLndpdGhQb3NpdGlvbnMocG9zaXRpb25zKVxuICAgICAgLndpdGhGbGV4aWJsZURpbWVuc2lvbnMoZmFsc2UpXG4gICAgICAud2l0aFB1c2goZmFsc2UpO1xuICAgIHJldHVybiB0aGlzLnBvc2l0aW9uU3RyYXRlZ3k7XG4gIH1cbn1cbiJdfQ==