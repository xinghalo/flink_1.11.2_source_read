/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ContentChildren, Input, QueryList, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
import { NzInputDirective } from './nz-input.directive';
var NzInputGroupComponent = /** @class */ (function () {
    function NzInputGroupComponent() {
        this._size = 'default';
        this.nzSearch = false;
        this.nzCompact = false;
    }
    Object.defineProperty(NzInputGroupComponent.prototype, "nzSize", {
        get: /**
         * @return {?}
         */
        function () {
            return this._size;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._size = value;
            this.updateChildrenInputSize();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isLarge", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzSize === 'large';
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isSmall", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzSize === 'small';
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isAffix", {
        get: /**
         * @return {?}
         */
        function () {
            return !!(this.nzSuffix || this.nzPrefix || this.nzPrefixIcon || this.nzSuffixIcon);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isAddOn", {
        get: /**
         * @return {?}
         */
        function () {
            return !!(this.nzAddOnAfter || this.nzAddOnBefore || this.nzAddOnAfterIcon || this.nzAddOnBeforeIcon);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isAffixWrapper", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isAffix && !this.isAddOn;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isGroup", {
        get: /**
         * @return {?}
         */
        function () {
            return !this.isAffix && !this.isAddOn;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isLargeGroup", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isGroup && this.isLarge;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isLargeGroupWrapper", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isAddOn && this.isLarge;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isLargeAffix", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isAffixWrapper && this.isLarge;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isLargeSearch", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzSearch && this.isLarge;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isSmallGroup", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isGroup && this.isSmall;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isSmallAffix", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isAffixWrapper && this.isSmall;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isSmallGroupWrapper", {
        get: /**
         * @return {?}
         */
        function () {
            return this.isAddOn && this.isSmall;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzInputGroupComponent.prototype, "isSmallSearch", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzSearch && this.isSmall;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzInputGroupComponent.prototype.updateChildrenInputSize = /**
     * @return {?}
     */
    function () {
        var _this = this;
        if (this.listOfNzInputDirective) {
            this.listOfNzInputDirective.forEach((/**
             * @param {?} item
             * @return {?}
             */
            function (item) { return (item.nzSize = _this.nzSize); }));
        }
    };
    /**
     * @return {?}
     */
    NzInputGroupComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        this.updateChildrenInputSize();
    };
    NzInputGroupComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-input-group',
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    template: "<span class=\"ant-input-wrapper ant-input-group\" *ngIf=\"isAddOn\">\n  <span class=\"ant-input-group-addon\" *ngIf=\"nzAddOnBefore || nzAddOnBeforeIcon\">\n    <i nz-icon [type]=\"nzAddOnBeforeIcon\" *ngIf=\"nzAddOnBeforeIcon\"></i>\n    <ng-container *nzStringTemplateOutlet=\"nzAddOnBefore\">{{ nzAddOnBefore }}</ng-container>\n  </span>\n  <ng-template [ngIf]=\"!isAffix\" *ngTemplateOutlet=\"contentTemplate\"></ng-template>\n  <span class=\"ant-input-affix-wrapper\" [class.ant-input-affix-wrapper-sm]=\"isSmall\" [class.ant-input-affix-wrapper-lg]=\"isLarge\" *ngIf=\"isAffix\">\n    <ng-template *ngTemplateOutlet=\"affixTemplate\"></ng-template>\n  </span>\n  <span class=\"ant-input-group-addon\" *ngIf=\"nzAddOnAfter || nzAddOnAfterIcon\">\n    <i nz-icon [type]=\"nzAddOnAfterIcon\" *ngIf=\"nzAddOnAfterIcon\"></i>\n    <ng-container *nzStringTemplateOutlet=\"nzAddOnAfter\">{{ nzAddOnAfter }}</ng-container>\n  </span>\n</span>\n<ng-container *ngIf=\"isAffix && !isAddOn\">\n  <ng-template *ngTemplateOutlet=\"affixTemplate\"></ng-template>\n</ng-container>\n<ng-template #affixTemplate>\n  <span class=\"ant-input-prefix\" *ngIf=\"nzPrefix || nzPrefixIcon\">\n    <!-- TODO: should have a class to set its color, cc: antd-->\n    <i nz-icon [type]=\"nzPrefixIcon\" *ngIf=\"nzPrefixIcon\" style=\"color: rgba(0, 0, 0, 0.25)\"></i>\n    <ng-container *nzStringTemplateOutlet=\"nzPrefix\">{{ nzPrefix }}</ng-container>\n  </span>\n  <ng-template *ngTemplateOutlet=\"contentTemplate\"></ng-template>\n  <span class=\"ant-input-suffix\" *ngIf=\"nzSuffix || nzSuffixIcon\">\n    <i nz-icon [type]=\"nzSuffixIcon\" *ngIf=\"nzSuffixIcon\"></i>\n    <ng-container *nzStringTemplateOutlet=\"nzSuffix\">{{ nzSuffix }}</ng-container>\n  </span>\n</ng-template>\n<ng-template [ngIf]=\"isGroup\" *ngTemplateOutlet=\"contentTemplate\"></ng-template>\n<ng-template #contentTemplate>\n  <ng-content></ng-content>\n</ng-template>",
                    host: {
                        '[class.ant-input-group-compact]': 'nzCompact',
                        '[class.ant-input-search-enter-button]': 'nzSearch',
                        '[class.ant-input-search]': 'nzSearch',
                        '[class.ant-input-search-sm]': 'isSmallSearch',
                        '[class.ant-input-affix-wrapper]': 'isAffixWrapper',
                        '[class.ant-input-group-wrapper]': 'isAddOn',
                        '[class.ant-input-group]': 'isGroup',
                        '[class.ant-input-group-lg]': 'isLargeGroup',
                        '[class.ant-input-group-wrapper-lg]': 'isLargeGroupWrapper',
                        '[class.ant-input-affix-wrapper-lg]': 'isLargeAffix',
                        '[class.ant-input-search-lg]': 'isLargeSearch',
                        '[class.ant-input-group-sm]': 'isSmallGroup',
                        '[class.ant-input-affix-wrapper-sm]': 'isSmallAffix',
                        '[class.ant-input-group-wrapper-sm]': 'isSmallGroupWrapper'
                    }
                }] }
    ];
    NzInputGroupComponent.propDecorators = {
        listOfNzInputDirective: [{ type: ContentChildren, args: [NzInputDirective,] }],
        nzAddOnBeforeIcon: [{ type: Input }],
        nzAddOnAfterIcon: [{ type: Input }],
        nzPrefixIcon: [{ type: Input }],
        nzSuffixIcon: [{ type: Input }],
        nzAddOnBefore: [{ type: Input }],
        nzAddOnAfter: [{ type: Input }],
        nzPrefix: [{ type: Input }],
        nzSuffix: [{ type: Input }],
        nzSearch: [{ type: Input }],
        nzCompact: [{ type: Input }],
        nzSize: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzInputGroupComponent.prototype, "nzSearch", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzInputGroupComponent.prototype, "nzCompact", void 0);
    return NzInputGroupComponent;
}());
export { NzInputGroupComponent };
if (false) {
    /** @type {?} */
    NzInputGroupComponent.prototype.listOfNzInputDirective;
    /**
     * @type {?}
     * @private
     */
    NzInputGroupComponent.prototype._size;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzAddOnBeforeIcon;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzAddOnAfterIcon;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzPrefixIcon;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzSuffixIcon;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzAddOnBefore;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzAddOnAfter;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzPrefix;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzSuffix;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzSearch;
    /** @type {?} */
    NzInputGroupComponent.prototype.nzCompact;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaW5wdXQtZ3JvdXAuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImlucHV0L256LWlucHV0LWdyb3VwLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULGVBQWUsRUFDZixLQUFLLEVBQ0wsU0FBUyxFQUVULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUd2QixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEQsT0FBTyxFQUFFLGdCQUFnQixFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFFeEQ7SUFBQTtRQXlCVSxVQUFLLEdBQWtCLFNBQVMsQ0FBQztRQVNoQixhQUFRLEdBQUcsS0FBSyxDQUFDO1FBQ2pCLGNBQVMsR0FBRyxLQUFLLENBQUM7SUE0RTdDLENBQUM7SUExRUMsc0JBQWEseUNBQU07Ozs7UUFLbkI7WUFDRSxPQUFPLElBQUksQ0FBQyxLQUFLLENBQUM7UUFDcEIsQ0FBQzs7Ozs7UUFQRCxVQUFvQixLQUFvQjtZQUN0QyxJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztZQUNuQixJQUFJLENBQUMsdUJBQXVCLEVBQUUsQ0FBQztRQUNqQyxDQUFDOzs7T0FBQTtJQU1ELHNCQUFJLDBDQUFPOzs7O1FBQVg7WUFDRSxPQUFPLElBQUksQ0FBQyxNQUFNLEtBQUssT0FBTyxDQUFDO1FBQ2pDLENBQUM7OztPQUFBO0lBRUQsc0JBQUksMENBQU87Ozs7UUFBWDtZQUNFLE9BQU8sSUFBSSxDQUFDLE1BQU0sS0FBSyxPQUFPLENBQUM7UUFDakMsQ0FBQzs7O09BQUE7SUFFRCxzQkFBSSwwQ0FBTzs7OztRQUFYO1lBQ0UsT0FBTyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsUUFBUSxJQUFJLElBQUksQ0FBQyxRQUFRLElBQUksSUFBSSxDQUFDLFlBQVksSUFBSSxJ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b0MsRUFBRSxxQkFBcUI7d0JBQzNELG9DQUFvQyxFQUFFLGNBQWM7d0JBQ3BELDZCQUE2QixFQUFFLGVBQWU7d0JBQzlDLDRCQUE0QixFQUFFLGNBQWM7d0JBQzVDLG9DQUFvQyxFQUFFLGNBQWM7d0JBQ3BELG9DQUFvQyxFQUFFLHFCQUFxQjtxQkFDNUQ7aUJBQ0Y7Ozt5Q0FFRSxlQUFlLFNBQUMsZ0JBQWdCO29DQUVoQyxLQUFLO21DQUNMLEtBQUs7K0JBQ0wsS0FBSzsrQkFDTCxLQUFLO2dDQUNMLEtBQUs7K0JBQ0wsS0FBSzsyQkFDTCxLQUFLOzJCQUNMLEtBQUs7MkJBQ0wsS0FBSzs0QkFDTCxLQUFLO3lCQUVMLEtBQUs7O0lBSG1CO1FBQWYsWUFBWSxFQUFFOzsyREFBa0I7SUFDakI7UUFBZixZQUFZLEVBQUU7OzREQUFtQjtJQTRFN0MsNEJBQUM7Q0FBQSxBQS9HRCxJQStHQztTQXhGWSxxQkFBcUI7OztJQUNoQyx1REFBdUY7Ozs7O0lBQ3ZGLHNDQUF5Qzs7SUFDekMsa0RBQXdDOztJQUN4QyxpREFBdUM7O0lBQ3ZDLDZDQUFtQzs7SUFDbkMsNkNBQW1DOztJQUNuQyw4Q0FBbUQ7O0lBQ25ELDZDQUFrRDs7SUFDbEQseUNBQThDOztJQUM5Qyx5Q0FBOEM7O0lBQzlDLHlDQUEwQzs7SUFDMUMsMENBQTJDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBJbnB1dCxcbiAgUXVlcnlMaXN0LFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOZ0NsYXNzVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvbmctY2xhc3MnO1xuaW1wb3J0IHsgTnpTaXplTERTVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvc2l6ZSc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBOeklucHV0RGlyZWN0aXZlIH0gZnJvbSAnLi9uei1pbnB1dC5kaXJlY3RpdmUnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1pbnB1dC1ncm91cCcsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWlucHV0LWdyb3VwLmNvbXBvbmVudC5odG1sJyxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LWlucHV0LWdyb3VwLWNvbXBhY3RdJzogJ256Q29tcGFjdCcsXG4gICAgJ1tjbGFzcy5hbnQtaW5wdXQtc2VhcmNoLWVudGVyLWJ1dHRvbl0nOiAnbnpTZWFyY2gnLFxuICAgICdbY2xhc3MuYW50LWlucHV0LXNlYXJjaF0nOiAnbnpTZWFyY2gnLFxuICAgICdbY2xhc3MuYW50LWlucHV0LXNlYXJjaC1zbV0nOiAnaXNTbWFsbFNlYXJjaCcsXG4gICAgJ1tjbGFzcy5hbnQtaW5wdXQtYWZmaXgtd3JhcHBlcl0nOiAnaXNBZmZpeFdyYXBwZXInLFxuICAgICdbY2xhc3MuYW50LWlucHV0LWdyb3VwLXdyYXBwZXJdJzogJ2lzQWRkT24nLFxuICAgICdbY2xhc3MuYW50LWlucHV0LWdyb3VwXSc6ICdpc0dyb3VwJyxcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1ncm91cC1sZ10nOiAnaXNMYXJnZUdyb3VwJyxcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1ncm91cC13cmFwcGVyLWxnXSc6ICdpc0xhcmdlR3JvdXBXcmFwcGVyJyxcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1hZmZpeC13cmFwcGVyLWxnXSc6ICdpc0xhcmdlQWZmaXgnLFxuICAgICdbY2xhc3MuYW50LWlucHV0LXNlYXJjaC1sZ10nOiAnaXNMYXJnZVNlYXJjaCcsXG4gICAgJ1tjbGFzcy5hbnQtaW5wdXQtZ3JvdXAtc21dJzogJ2lzU21hbGxHcm91cCcsXG4gICAgJ1tjbGFzcy5hbnQtaW5wdXQtYWZmaXgtd3JhcHBlci1zbV0nOiAnaXNTbWFsbEFmZml4JyxcbiAgICAnW2NsYXNzLmFudC1pbnB1dC1ncm91cC13cmFwcGVyLXNtXSc6ICdpc1NtYWxsR3JvdXBXcmFwcGVyJ1xuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56SW5wdXRHcm91cENvbXBvbmVudCBpbXBsZW1lbnRzIEFmdGVyQ29udGVudEluaXQge1xuICBAQ29udGVudENoaWxkcmVuKE56SW5wdXREaXJlY3RpdmUpIGxpc3RPZk56SW5wdXREaXJlY3RpdmU6IFF1ZXJ5TGlzdDxOeklucHV0RGlyZWN0aXZlPjtcbiAgcHJpdmF0ZSBfc2l6ZTogTnpTaXplTERTVHlwZSA9ICdkZWZhdWx0JztcbiAgQElucHV0KCkgbnpBZGRPbkJlZm9yZUljb246IE5nQ2xhc3NUeXBlO1xuICBASW5wdXQoKSBuekFkZE9uQWZ0ZXJJY29uOiBOZ0NsYXNzVHlwZTtcbiAgQElucHV0KCkgbnpQcmVmaXhJY29uOiBOZ0NsYXNzVHlwZTtcbiAgQElucHV0KCkgbnpTdWZmaXhJY29uOiBOZ0NsYXNzVHlwZTtcbiAgQElucHV0KCkgbnpBZGRPbkJlZm9yZTogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56QWRkT25BZnRlcjogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56UHJlZml4OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpTdWZmaXg6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTZWFyY2ggPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56Q29tcGFjdCA9IGZhbHNlO1xuXG4gIEBJbnB1dCgpIHNldCBuelNpemUodmFsdWU6IE56U2l6ZUxEU1R5cGUpIHtcbiAgICB0aGlzLl9zaXplID0gdmFsdWU7XG4gICAgdGhpcy51cGRhdGVDaGlsZHJlbklucHV0U2l6ZSgpO1xuICB9XG5cbiAgZ2V0IG56U2l6ZSgpOiBOelNpemVMRFNUeXBlIHtcbiAgICByZXR1cm4gdGhpcy5fc2l6ZTtcbiAgfVxuXG4gIGdldCBpc0xhcmdlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLm56U2l6ZSA9PT0gJ2xhcmdlJztcbiAgfVxuXG4gIGdldCBpc1NtYWxsKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLm56U2l6ZSA9PT0gJ3NtYWxsJztcbiAgfVxuXG4gIGdldCBpc0FmZml4KCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAhISh0aGlzLm56U3VmZml4IHx8IHRoaXMubnpQcmVmaXggfHwgdGhpcy5uelByZWZpeEljb24gfHwgdGhpcy5uelN1ZmZpeEljb24pO1xuICB9XG5cbiAgZ2V0IGlzQWRkT24oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuICEhKHRoaXMubnpBZGRPbkFmdGVyIHx8IHRoaXMubnpBZGRPbkJlZm9yZSB8fCB0aGlzLm56QWRkT25BZnRlckljb24gfHwgdGhpcy5uekFkZE9uQmVmb3JlSWNvbik7XG4gIH1cblxuICBnZXQgaXNBZmZpeFdyYXBwZXIoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuaXNBZmZpeCAmJiAhdGhpcy5pc0FkZE9uO1xuICB9XG5cbiAgZ2V0IGlzR3JvdXAoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuICF0aGlzLmlzQWZmaXggJiYgIXRoaXMuaXNBZGRPbjtcbiAgfVxuXG4gIGdldCBpc0xhcmdlR3JvdXAoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuaXNHcm91cCAmJiB0aGlzLmlzTGFyZ2U7XG4gIH1cblxuICBnZXQgaXNMYXJnZUdyb3VwV3JhcHBlcigpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5pc0FkZE9uICYmIHRoaXMuaXNMYXJnZTtcbiAgfVxuXG4gIGdldCBpc0xhcmdlQWZmaXgoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuaXNBZmZpeFdyYXBwZXIgJiYgdGhpcy5pc0xhcmdlO1xuICB9XG5cbiAgZ2V0IGlzTGFyZ2VTZWFyY2goKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMubnpTZWFyY2ggJiYgdGhpcy5pc0xhcmdlO1xuICB9XG5cbiAgZ2V0IGlzU21hbGxHcm91cCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5pc0dyb3VwICYmIHRoaXMuaXNTbWFsbDtcbiAgfVxuXG4gIGdldCBpc1NtYWxsQWZmaXgoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuaXNBZmZpeFdyYXBwZXIgJiYgdGhpcy5pc1NtYWxsO1xuICB9XG5cbiAgZ2V0IGlzU21hbGxHcm91cFdyYXBwZXIoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuaXNBZGRPbiAmJiB0aGlzLmlzU21hbGw7XG4gIH1cblxuICBnZXQgaXNTbWFsbFNlYXJjaCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5uelNlYXJjaCAmJiB0aGlzLmlzU21hbGw7XG4gIH1cblxuICB1cGRhdGVDaGlsZHJlbklucHV0U2l6ZSgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5saXN0T2ZOeklucHV0RGlyZWN0aXZlKSB7XG4gICAgICB0aGlzLmxpc3RPZk56SW5wdXREaXJlY3RpdmUuZm9yRWFjaChpdGVtID0+IChpdGVtLm56U2l6ZSA9IHRoaXMubnpTaXplKSk7XG4gICAgfVxuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIHRoaXMudXBkYXRlQ2hpbGRyZW5JbnB1dFNpemUoKTtcbiAgfVxufVxuIl19