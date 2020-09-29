/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { DOCUMENT } from '@angular/common';
import { HttpBackend } from '@angular/common/http';
import { Inject, Injectable, InjectionToken, Optional, RendererFactory2 } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { IconService } from '@ant-design/icons-angular';
import { BarsOutline, CalendarOutline, CaretDownFill, CaretDownOutline, CaretUpFill, CaretUpOutline, CheckCircleFill, CheckCircleOutline, CheckOutline, ClockCircleOutline, CloseCircleFill, CloseCircleOutline, CloseOutline, DoubleLeftOutline, DoubleRightOutline, DownOutline, EllipsisOutline, ExclamationCircleFill, ExclamationCircleOutline, EyeOutline, FileFill, FileOutline, FilterFill, InfoCircleFill, InfoCircleOutline, LeftOutline, LoadingOutline, PaperClipOutline, QuestionCircleOutline, RightOutline, SearchOutline, StarFill, UploadOutline, UpOutline } from '@ant-design/icons-angular/icons';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/platform-browser";
import * as i3 from "@angular/common/http";
import * as i4 from "@angular/common";
/**
 * @record
 */
export function NzIconfontOption() { }
if (false) {
    /** @type {?} */
    NzIconfontOption.prototype.scriptUrl;
}
/** @type {?} */
export var NZ_ICONS = new InjectionToken('nz_icons');
/** @type {?} */
export var NZ_ICON_DEFAULT_TWOTONE_COLOR = new InjectionToken('nz_icon_default_twotone_color');
/** @type {?} */
export var DEFAULT_TWOTONE_COLOR = '#1890ff';
/** @type {?} */
export var NZ_ICONS_USED_BY_ZORRO = [
    BarsOutline,
    CalendarOutline,
    CaretUpFill,
    CaretUpOutline,
    CaretDownFill,
    CaretDownOutline,
    CheckCircleFill,
    CheckCircleOutline,
    CheckOutline,
    ClockCircleOutline,
    CloseCircleOutline,
    CloseCircleFill,
    CloseOutline,
    DoubleLeftOutline,
    DoubleRightOutline,
    DownOutline,
    EllipsisOutline,
    ExclamationCircleFill,
    ExclamationCircleOutline,
    EyeOutline,
    FileFill,
    FileOutline,
    FilterFill,
    InfoCircleFill,
    InfoCircleOutline,
    LeftOutline,
    LoadingOutline,
    PaperClipOutline,
    QuestionCircleOutline,
    RightOutline,
    StarFill,
    SearchOutline,
    StarFill,
    UploadOutline,
    UpOutline
];
/**
 * It should be a global singleton, otherwise registered icons could not be found.
 */
var NzIconService = /** @class */ (function (_super) {
    tslib_1.__extends(NzIconService, _super);
    function NzIconService(rendererFactory, sanitizer, handler, document, icons, defaultColor) {
        var _this = _super.call(this, rendererFactory, handler, document, sanitizer) || this;
        _this.rendererFactory = rendererFactory;
        _this.sanitizer = sanitizer;
        _this.handler = handler;
        _this.document = document;
        _this.icons = icons;
        _this.defaultColor = defaultColor;
        _this.iconfontCache = new Set();
        _this.warnedAboutAPI = false;
        _this.warnedAboutCross = false;
        _this.warnedAboutVertical = false;
        _this.addIcon.apply(_this, tslib_1.__spread(NZ_ICONS_USED_BY_ZORRO, (_this.icons || [])));
        /** @type {?} */
        var primaryColor = DEFAULT_TWOTONE_COLOR;
        if (_this.defaultColor) {
            if (_this.defaultColor.startsWith('#')) {
                primaryColor = _this.defaultColor;
            }
            else {
                console.warn('[NG-ZORRO]: twotone color must be a hex color!');
            }
        }
        _this.twoToneColor = { primaryColor: primaryColor };
        return _this;
    }
    /**
     * @param {?} type
     * @return {?}
     */
    NzIconService.prototype.warnAPI = /**
     * @param {?} type
     * @return {?}
     */
    function (type) {
        if (type === 'old' && !this.warnedAboutAPI) {
            console.warn("<i class=\"anticon\"></i> would be deprecated soon. Please use <i nz-icon type=\"\"></i> API.");
            this.warnedAboutAPI = true;
        }
        if (type === 'cross' && !this.warnedAboutCross) {
            console.warn("'cross' icon is replaced by 'close' icon.");
            this.warnedAboutCross = true;
        }
        if (type === 'vertical' && !this.warnedAboutVertical) {
            console.warn("'verticle' is misspelled, would be corrected in the next major version.");
            this.warnedAboutVertical = true;
        }
    };
    /**
     * @param {?} svg
     * @return {?}
     */
    NzIconService.prototype.normalizeSvgElement = /**
     * @param {?} svg
     * @return {?}
     */
    function (svg) {
        if (!svg.getAttribute('viewBox')) {
            this._renderer.setAttribute(svg, 'viewBox', '0 0 1024 1024');
        }
        if (!svg.getAttribute('width') || !svg.getAttribute('height')) {
            this._renderer.setAttribute(svg, 'width', '1em');
            this._renderer.setAttribute(svg, 'height', '1em');
        }
        if (!svg.getAttribute('fill')) {
            this._renderer.setAttribute(svg, 'fill', 'currentColor');
        }
    };
    /**
     * @param {?} opt
     * @return {?}
     */
    NzIconService.prototype.fetchFromIconfont = /**
     * @param {?} opt
     * @return {?}
     */
    function (opt) {
        var scriptUrl = opt.scriptUrl;
        if (this.document && !this.iconfontCache.has(scriptUrl)) {
            /** @type {?} */
            var script = this._renderer.createElement('script');
            this._renderer.setAttribute(script, 'src', scriptUrl);
            this._renderer.setAttribute(script, 'data-namespace', scriptUrl.replace(/^(https?|http):/g, ''));
            this._renderer.appendChild(this.document.body, script);
            this.iconfontCache.add(scriptUrl);
        }
    };
    /**
     * @param {?} type
     * @return {?}
     */
    NzIconService.prototype.createIconfontIcon = /**
     * @param {?} type
     * @return {?}
     */
    function (type) {
        return this._createSVGElementFromString("<svg><use xlink:href=\"" + type + "\"></svg>");
    };
    NzIconService.decorators = [
        { type: Injectable, args: [{
                    providedIn: 'root'
                },] }
    ];
    /** @nocollapse */
    NzIconService.ctorParameters = function () { return [
        { type: RendererFactory2 },
        { type: DomSanitizer },
        { type: HttpBackend, decorators: [{ type: Optional }] },
        { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [DOCUMENT,] }] },
        { type: Array, decorators: [{ type: Optional }, { type: Inject, args: [NZ_ICONS,] }] },
        { type: String, decorators: [{ type: Optional }, { type: Inject, args: [NZ_ICON_DEFAULT_TWOTONE_COLOR,] }] }
    ]; };
    /** @nocollapse */ NzIconService.ngInjectableDef = i0.defineInjectable({ factory: function NzIconService_Factory() { return new i1.NzIconService(i0.inject(i0.RendererFactory2), i0.inject(i2.DomSanitizer), i0.inject(i3.HttpBackend, 8), i0.inject(i4.DOCUMENT, 8), i0.inject(i1.NZ_ICONS, 8), i0.inject(i1.NZ_ICON_DEFAULT_TWOTONE_COLOR, 8)); }, token: i1.NzIconService, providedIn: "root" });
    return NzIconService;
}(IconService));
export { NzIconService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.iconfontCache;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.warnedAboutAPI;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.warnedAboutCross;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.warnedAboutVertical;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.rendererFactory;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.sanitizer;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.handler;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.document;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.icons;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.defaultColor;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaWNvbi5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImljb24vbnotaWNvbi5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQzNDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNuRCxPQUFPLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxjQUFjLEVBQUUsUUFBUSxFQUFFLGdCQUFnQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQy9GLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSwyQkFBMkIsQ0FBQztBQUN6RCxPQUFPLEVBQWtCLFdBQVcsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ3hFLE9BQU8sRUFDTCxXQUFXLEVBQ1gsZUFBZSxFQUNmLGFBQWEsRUFDYixnQkFBZ0IsRUFDaEIsV0FBVyxFQUNYLGNBQWMsRUFDZCxlQUFlLEVBQ2Ysa0JBQWtCLEVBQ2xCLFlBQVksRUFDWixrQkFBa0IsRUFDbEIsZUFBZSxFQUNmLGtCQUFrQixFQUNsQixZQUFZLEVBQ1osaUJBQWlCLEVBQ2pCLGtCQUFrQixFQUNsQixXQUFXLEVBQ1gsZUFBZSxFQUNmLHFCQUFxQixFQUNyQix3QkFBd0IsRUFDeEIsVUFBVSxFQUNWLFFBQVEsRUFDUixXQUFXLEVBQ1gsVUFBVSxFQUNWLGNBQWMsRUFDZCxpQkFBaUIsRUFDakIsV0FBVyxFQUNYLGNBQWMsRUFDZCxnQkFBZ0IsRUFDaEIscUJBQXFCLEVBQ3JCLFlBQVksRUFDWixhQUFhLEVBQ2IsUUFBUSxFQUNSLGFBQWEsRUFDYixTQUFTLEVBQ1YsTUFBTSxpQ0FBaUMsQ0FBQzs7Ozs7Ozs7O0FBRXpDLHNDQUVDOzs7SUFEQyxxQ0FBa0I7OztBQUdwQixNQUFNLEtBQU8sUUFBUSxHQUFHLElBQUksY0FBYyxDQUFDLFVBQVUsQ0FBQzs7QUFDdEQsTUFB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QyxTQUFTLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxFQUFFLE1BQU0sQ0FBQyxDQUFDO1lBQ3ZELElBQUksQ0FBQyxhQUFhLENBQUMsR0FBRyxDQUFDLFNBQVMsQ0FBQyxDQUFDO1NBQ25DO0lBQ0gsQ0FBQzs7Ozs7SUFFRCwwQ0FBa0I7Ozs7SUFBbEIsVUFBbUIsSUFBWTtRQUM3QixPQUFPLElBQUksQ0FBQywyQkFBMkIsQ0FBQyw0QkFBeUIsSUFBSSxjQUFVLENBQUMsQ0FBQztJQUNuRixDQUFDOztnQkFsREYsVUFBVSxTQUFDO29CQUNWLFVBQVUsRUFBRSxNQUFNO2lCQUNuQjs7OztnQkExRnNELGdCQUFnQjtnQkFDOUQsWUFBWTtnQkFGWixXQUFXLHVCQWdKZixRQUFRO2dEQUVSLFFBQVEsWUFBSSxNQUFNLFNBQUMsUUFBUTs0Q0FDM0IsUUFBUSxZQUFJLE1BQU0sU0FBQyxRQUFROzZDQUMzQixRQUFRLFlBQUksTUFBTSxTQUFDLDZCQUE2Qjs7O3dCQXJKckQ7Q0FxS0MsQUEzRUQsQ0FHbUMsV0FBVyxHQXdFN0M7U0F4RVksYUFBYTs7Ozs7O0lBQ3hCLHNDQUEwQzs7Ozs7SUFDMUMsdUNBQStCOzs7OztJQUMvQix5Q0FBaUM7Ozs7O0lBQ2pDLDRDQUFvQzs7Ozs7SUE4Q2xDLHdDQUEyQzs7Ozs7SUFDM0Msa0NBQWlDOzs7OztJQUNqQyxnQ0FBMEM7Ozs7O0lBRTFDLGlDQUFxRDs7Ozs7SUFDckQsOEJBQTZEOzs7OztJQUM3RCxxQ0FBK0UiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBET0NVTUVOVCB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQgeyBIdHRwQmFja2VuZCB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbi9odHRwJztcbmltcG9ydCB7IEluamVjdCwgSW5qZWN0YWJsZSwgSW5qZWN0aW9uVG9rZW4sIE9wdGlvbmFsLCBSZW5kZXJlckZhY3RvcnkyIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBEb21TYW5pdGl6ZXIgfSBmcm9tICdAYW5ndWxhci9wbGF0Zm9ybS1icm93c2VyJztcbmltcG9ydCB7IEljb25EZWZpbml0aW9uLCBJY29uU2VydmljZSB9IGZyb20gJ0BhbnQtZGVzaWduL2ljb25zLWFuZ3VsYXInO1xuaW1wb3J0IHtcbiAgQmFyc091dGxpbmUsXG4gIENhbGVuZGFyT3V0bGluZSxcbiAgQ2FyZXREb3duRmlsbCxcbiAgQ2FyZXREb3duT3V0bGluZSxcbiAgQ2FyZXRVcEZpbGwsXG4gIENhcmV0VXBPdXRsaW5lLFxuICBDaGVja0NpcmNsZUZpbGwsXG4gIENoZWNrQ2lyY2xlT3V0bGluZSxcbiAgQ2hlY2tPdXRsaW5lLFxuICBDbG9ja0NpcmNsZU91dGxpbmUsXG4gIENsb3NlQ2lyY2xlRmlsbCxcbiAgQ2xvc2VDaXJjbGVPdXRsaW5lLFxuICBDbG9zZU91dGxpbmUsXG4gIERvdWJsZUxlZnRPdXRsaW5lLFxuICBEb3VibGVSaWdodE91dGxpbmUsXG4gIERvd25PdXRsaW5lLFxuICBFbGxpcHNpc091dGxpbmUsXG4gIEV4Y2xhbWF0aW9uQ2lyY2xlRmlsbCxcbiAgRXhjbGFtYXRpb25DaXJjbGVPdXRsaW5lLFxuICBFeWVPdXRsaW5lLFxuICBGaWxlRmlsbCxcbiAgRmlsZU91dGxpbmUsXG4gIEZpbHRlckZpbGwsXG4gIEluZm9DaXJjbGVGaWxsLFxuICBJbmZvQ2lyY2xlT3V0bGluZSxcbiAgTGVmdE91dGxpbmUsXG4gIExvYWRpbmdPdXRsaW5lLFxuICBQYXBlckNsaXBPdXRsaW5lLFxuICBRdWVzdGlvbkNpcmNsZU91dGxpbmUsXG4gIFJpZ2h0T3V0bGluZSxcbiAgU2VhcmNoT3V0bGluZSxcbiAgU3RhckZpbGwsXG4gIFVwbG9hZE91dGxpbmUsXG4gIFVwT3V0bGluZVxufSBmcm9tICdAYW50LWRlc2lnbi9pY29ucy1hbmd1bGFyL2ljb25zJztcblxuZXhwb3J0IGludGVyZmFjZSBOekljb25mb250T3B0aW9uIHtcbiAgc2NyaXB0VXJsOiBzdHJpbmc7XG59XG5cbmV4cG9ydCBjb25zdCBOWl9JQ09OUyA9IG5ldyBJbmplY3Rpb25Ub2tlbignbnpfaWNvbnMnKTtcbmV4cG9ydCBjb25zdCBOWl9JQ09OX0RFRkFVTFRfVFdPVE9ORV9DT0xPUiA9IG5ldyBJbmplY3Rpb25Ub2tlbignbnpfaWNvbl9kZWZhdWx0X3R3b3RvbmVfY29sb3InKTtcbmV4cG9ydCBjb25zdCBERUZBVUxUX1RXT1RPTkVfQ09MT1IgPSAnIzE4OTBmZic7XG5leHBvcnQgY29uc3QgTlpfSUNPTlNfVVNFRF9CWV9aT1JSTzogSWNvbkRlZmluaXRpb25bXSA9IFtcbiAgQmFyc091dGxpbmUsXG4gIENhbGVuZGFyT3V0bGluZSxcbiAgQ2FyZXRVcEZpbGwsXG4gIENhcmV0VXBPdXRsaW5lLFxuICBDYXJldERvd25GaWxsLFxuICBDYXJldERvd25PdXRsaW5lLFxuICBDaGVja0NpcmNsZUZpbGwsXG4gIENoZWNrQ2lyY2xlT3V0bGluZSxcbiAgQ2hlY2tPdXRsaW5lLFxuICBDbG9ja0NpcmNsZU91dGxpbmUsXG4gIENsb3NlQ2lyY2xlT3V0bGluZSxcbiAgQ2xvc2VDaXJjbGVGaWxsLFxuICBDbG9zZU91dGxpbmUsXG4gIERvdWJsZUxlZnRPdXRsaW5lLFxuICBEb3VibGVSaWdodE91dGxpbmUsXG4gIERvd25PdXRsaW5lLFxuICBFbGxpcHNpc091dGxpbmUsXG4gIEV4Y2xhbWF0aW9uQ2lyY2xlRmlsbCxcbiAgRXhjbGFtYXRpb25DaXJjbGVPdXRsaW5lLFxuICBFeWVPdXRsaW5lLFxuICBGaWxlRmlsbCxcbiAgRmlsZU91dGxpbmUsXG4gIEZpbHRlckZpbGwsXG4gIEluZm9DaXJjbGVGaWxsLFxuICBJbmZvQ2lyY2xlT3V0bGluZSxcbiAgTGVmdE91dGxpbmUsXG4gIExvYWRpbmdPdXRsaW5lLFxuICBQYXBlckNsaXBPdXRsaW5lLFxuICBRdWVzdGlvbkNpcmNsZU91dGxpbmUsXG4gIFJpZ2h0T3V0bGluZSxcbiAgU3RhckZpbGwsXG4gIFNlYXJjaE91dGxpbmUsXG4gIFN0YXJGaWxsLFxuICBVcGxvYWRPdXRsaW5lLFxuICBVcE91dGxpbmVcbl07XG5cbi8qKlxuICogSXQgc2hvdWxkIGJlIGEgZ2xvYmFsIHNpbmdsZXRvbiwgb3RoZXJ3aXNlIHJlZ2lzdGVyZWQgaWNvbnMgY291bGQgbm90IGJlIGZvdW5kLlxuICovXG5ASW5qZWN0YWJsZSh7XG4gIHByb3ZpZGVkSW46ICdyb290J1xufSlcbmV4cG9ydCBjbGFzcyBOekljb25TZXJ2aWNlIGV4dGVuZHMgSWNvblNlcnZpY2Uge1xuICBwcml2YXRlIGljb25mb250Q2FjaGUgPSBuZXcgU2V0PHN0cmluZz4oKTtcbiAgcHJpdmF0ZSB3YXJuZWRBYm91dEFQSSA9IGZhbHNlO1xuICBwcml2YXRlIHdhcm5lZEFib3V0Q3Jvc3MgPSBmYWxzZTtcbiAgcHJpdmF0ZSB3YXJuZWRBYm91dFZlcnRpY2FsID0gZmFsc2U7XG5cbiAgd2FybkFQSSh0eXBlOiAnb2xkJyB8ICdjcm9zcycgfCAndmVydGljYWwnKTogdm9pZCB7XG4gICAgaWYgKHR5cGUgPT09ICdvbGQnICYmICF0aGlzLndhcm5lZEFib3V0QVBJKSB7XG4gICAgICBjb25zb2xlLndhcm4oYDxpIGNsYXNzPVwiYW50aWNvblwiPjwvaT4gd291bGQgYmUgZGVwcmVjYXRlZCBzb29uLiBQbGVhc2UgdXNlIDxpIG56LWljb24gdHlwZT1cIlwiPjwvaT4gQVBJLmApO1xuICAgICAgdGhpcy53YXJuZWRBYm91dEFQSSA9IHRydWU7XG4gICAgfVxuICAgIGlmICh0eXBlID09PSAnY3Jvc3MnICYmICF0aGlzLndhcm5lZEFib3V0Q3Jvc3MpIHtcbiAgICAgIGNvbnNvbGUud2FybihgJ2Nyb3NzJyBpY29uIGlzIHJlcGxhY2VkIGJ5ICdjbG9zZScgaWNvbi5gKTtcbiAgICAgIHRoaXMud2FybmVkQWJvdXRDcm9zcyA9IHRydWU7XG4gICAgfVxuICAgIGlmICh0eXBlID09PSAndmVydGljYWwnICYmICF0aGlzLndhcm5lZEFib3V0VmVydGljYWwpIHtcbiAgICAgIGNvbnNvbGUud2FybihgJ3ZlcnRpY2xlJyBpcyBtaXNzcGVsbGVkLCB3b3VsZCBiZSBjb3JyZWN0ZWQgaW4gdGhlIG5leHQgbWFqb3IgdmVyc2lvbi5gKTtcbiAgICAgIHRoaXMud2FybmVkQWJvdXRWZXJ0aWNhbCA9IHRydWU7XG4gICAgfVxuICB9XG5cbiAgbm9ybWFsaXplU3ZnRWxlbWVudChzdmc6IFNWR0VsZW1lbnQpOiB2b2lkIHtcbiAgICBpZiAoIXN2Zy5nZXRBdHRyaWJ1dGUoJ3ZpZXdCb3gnKSkge1xuICAgICAgdGhpcy5fcmVuZGVyZXIuc2V0QXR0cmlidXRlKHN2ZywgJ3ZpZXdCb3gnLCAnMCAwIDEwMjQgMTAyNCcpO1xuICAgIH1cbiAgICBpZiAoIXN2Zy5nZXRBdHRyaWJ1dGUoJ3dpZHRoJykgfHwgIXN2Zy5nZXRBdHRyaWJ1dGUoJ2hlaWdodCcpKSB7XG4gICAgICB0aGlzLl9yZW5kZXJlci5zZXRBdHRyaWJ1dGUoc3ZnLCAnd2lkdGgnLCAnMWVtJyk7XG4gICAgICB0aGlzLl9yZW5kZXJlci5zZXRBdHRyaWJ1dGUoc3ZnLCAnaGVpZ2h0JywgJzFlbScpO1xuICAgIH1cbiAgICBpZiAoIXN2Zy5nZXRBdHRyaWJ1dGUoJ2ZpbGwnKSkge1xuICAgICAgdGhpcy5fcmVuZGVyZXIuc2V0QXR0cmlidXRlKHN2ZywgJ2ZpbGwnLCAnY3VycmVudENvbG9yJyk7XG4gICAgfVxuICB9XG5cbiAgZmV0Y2hGcm9tSWNvbmZvbnQob3B0OiBOekljb25mb250T3B0aW9uKTogdm9pZCB7XG4gICAgY29uc3QgeyBzY3JpcHRVcmwgfSA9IG9wdDtcbiAgICBpZiAodGhpcy5kb2N1bWVudCAmJiAhdGhpcy5pY29uZm9udENhY2hlLmhhcyhzY3JpcHRVcmwpKSB7XG4gICAgICBjb25zdCBzY3JpcHQgPSB0aGlzLl9yZW5kZXJlci5jcmVhdGVFbGVtZW50KCdzY3JpcHQnKTtcbiAgICAgIHRoaXMuX3JlbmRlcmVyLnNldEF0dHJpYnV0ZShzY3JpcHQsICdzcmMnLCBzY3JpcHRVcmwpO1xuICAgICAgdGhpcy5fcmVuZGVyZXIuc2V0QXR0cmlidXRlKHNjcmlwdCwgJ2RhdGEtbmFtZXNwYWNlJywgc2NyaXB0VXJsLnJlcGxhY2UoL14oaHR0cHM/fGh0dHApOi9nLCAnJykpO1xuICAgICAgdGhpcy5fcmVuZGVyZXIuYXBwZW5kQ2hpbGQodGhpcy5kb2N1bWVudC5ib2R5LCBzY3JpcHQpO1xuICAgICAgdGhpcy5pY29uZm9udENhY2hlLmFkZChzY3JpcHRVcmwpO1xuICAgIH1cbiAgfVxuXG4gIGNyZWF0ZUljb25mb250SWNvbih0eXBlOiBzdHJpbmcpOiBTVkdFbGVtZW50IHtcbiAgICByZXR1cm4gdGhpcy5fY3JlYXRlU1ZHRWxlbWVudEZyb21TdHJpbmcoYDxzdmc+PHVzZSB4bGluazpocmVmPVwiJHt0eXBlfVwiPjwvc3ZnPmApO1xuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHJvdGVjdGVkIHJlbmRlcmVyRmFjdG9yeTogUmVuZGVyZXJGYWN0b3J5MixcbiAgICBwcm90ZWN0ZWQgc2FuaXRpemVyOiBEb21TYW5pdGl6ZXIsXG4gICAgQE9wdGlvbmFsKCkgcHJvdGVjdGVkIGhhbmRsZXI6IEh0dHBCYWNrZW5kLFxuICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgICBAT3B0aW9uYWwoKSBASW5qZWN0KERPQ1VNRU5UKSBwcm90ZWN0ZWQgZG9jdW1lbnQ6IGFueSxcbiAgICBAT3B0aW9uYWwoKSBASW5qZWN0KE5aX0lDT05TKSBwcml2YXRlIGljb25zOiBJY29uRGVmaW5pdGlvbltdLFxuICAgIEBPcHRpb25hbCgpIEBJbmplY3QoTlpfSUNPTl9ERUZBVUxUX1RXT1RPTkVfQ09MT1IpIHByaXZhdGUgZGVmYXVsdENvbG9yOiBzdHJpbmdcbiAgKSB7XG4gICAgc3VwZXIocmVuZGVyZXJGYWN0b3J5LCBoYW5kbGVyLCBkb2N1bWVudCwgc2FuaXRpemVyKTtcblxuICAgIHRoaXMuYWRkSWNvbiguLi5OWl9JQ09OU19VU0VEX0JZX1pPUlJPLCAuLi4odGhpcy5pY29ucyB8fCBbXSkpO1xuXG4gICAgbGV0IHByaW1hcnlDb2xvciA9IERFRkFVTFRfVFdPVE9ORV9DT0xPUjtcbiAgICBpZiAodGhpcy5kZWZhdWx0Q29sb3IpIHtcbiAgICAgIGlmICh0aGlzLmRlZmF1bHRDb2xvci5zdGFydHNXaXRoKCcjJykpIHtcbiAgICAgICAgcHJpbWFyeUNvbG9yID0gdGhpcy5kZWZhdWx0Q29sb3I7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oJ1tORy1aT1JST106IHR3b3RvbmUgY29sb3IgbXVzdCBiZSBhIGhleCBjb2xvciEnKTtcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy50d29Ub25lQ29sb3IgPSB7IHByaW1hcnlDb2xvciB9O1xuICB9XG59XG4iXX0=